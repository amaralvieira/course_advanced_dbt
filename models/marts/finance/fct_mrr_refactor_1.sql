{{ config(tags="monthly") }}

-- This model is created following the dbt MRR playbook: https://www.getdbt.com/blog/modeling-subscription-revenue/
-- Import CTEs
with subscriptions as (

    select * from {{ ref('dim_subscriptions') }}
    where
        billing_period = 'monthly'

),

-- Use the dates spine to generate a list of months
months as (

    select calendar_date as date_month
    from {{ ref('int_dates') }}
    where
        day_of_month = 1

),

monthly_subscriptions as (

    select
        subscription_id,
        user_id,
        starts_at,
        ends_at,
        plan_name,
        pricing as monthly_amount,
        {{ dbt.date_trunc("month", "starts_at") }}::date as start_month,
        case when ends_at is null
            then null
            else
                {{ dbt.date_trunc("month", "ends_at" ) }}::date
        end as end_month

    from subscriptions

),

windowed as (

    select
        *,
        datediff(month, end_month, lead(start_month,1) over (partition by user_id order by start_month asc)) as month_diff_to_next,
        case when
            row_number() over (partition by user_id order by start_month asc) = 1
            then 'new'
            when
            datediff(month, lag(end_month,1) over (partition by user_id order by start_month asc), start_month)
            in (0, 1) then
                case when monthly_amount - lag(monthly_amount) over (partition by user_id order by start_month asc) >0 then 'upgrade'
                    when monthly_amount - lag(monthly_amount) over (partition by user_id order by start_month asc) =0 then 'renewal'
                    else 'downgrade'
                end
            else 'reactivation'
        end as change_category,
        first_value(start_month) over (partition by user_id order by start_month asc)
         as first_subscription_month
    from monthly_subscriptions

),

final as (
    select
        months.date_month,
        windowed.subscription_id,
        windowed.user_id,
        windowed.starts_at,
        windowed.ends_at,
        windowed.plan_name,
        windowed.monthly_amount,
        windowed.start_month,
        windowed.end_month,
        windowed.change_category,
        windowed.first_subscription_month,
         {{ dbt.datediff("first_subscription_month", "months.date_month", "month") }} as month_retained_number
    from windowed
    inner join months
        on months.date_month between windowed.start_month and windowed.end_month

    union all

     select
        end_month as date_month,
        subscription_id,
        user_id,
        starts_at,
        ends_at,
        plan_name,
        - monthly_amount as monthly_amount,
        start_month,
        end_month,
        change_category,
        first_subscription_month,
        {{ dbt.datediff("first_subscription_month", "end_month", "month") }} as month_retained_number
    from windowed
    where end_month is not null

    union all

    select
        {{- dbt.dateadd("month", 1, "end_month") -}} as date_month,
        subscription_id,
        user_id,
        starts_at,
        ends_at,
        plan_name,
        - monthly_amount as monthly_amount,
        start_month,
        end_month,
        'churn' as change_category,
        first_subscription_month,
        null as month_retained_number
    from windowed
    where end_month is not null
    and ( month_diff_to_next > 1 or month_diff_to_next is null)
)

select * from final
