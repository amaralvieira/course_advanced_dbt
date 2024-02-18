{{ config(tags="monthly") }}

-- This model is created following the dbt MRR playbook: https://www.getdbt.com/blog/modeling-subscription-revenue/
-- Import CTEs
with subscriptions as (

    select * from {{ ref('dim_subscriptions') }}
    where
        billing_period = 'monthly'

),

monthly_subscriptions as (

    select
        user_id,
        subscription_id,
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


unioned as (
    select
        start_month as date_month,
        change_category,
        first_subscription_month,
        false as is_churn,
        monthly_amount as _in,
        0 as _out
    from windowed

    union all

     select
        case when month_diff_to_next = 0 then end_month
             else
             {{- dbt.dateadd("month", 1, "end_month") -}}
        end as date_month,
        change_category,
        first_subscription_month,
        case when month_diff_to_next > 1 or month_diff_to_next is null
            then true
            else false
        end as is_churn,
        0 as _in,
        monthly_amount as _out
    from windowed
    where end_month is not null

),

final as (
    select
        date_month,
        change_category,
        first_subscription_month,
        is_churn,
        sum(_in) as _in,
        sum(_out) as _out
    from unioned
    group by 1, 2, 3, 4
)

select * from final
