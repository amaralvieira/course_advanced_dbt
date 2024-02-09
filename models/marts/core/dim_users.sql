with

users as (
    select
        *
    from
        {{ ref('stg_bingeflix__users') }}
),

users_subscription_facts as (
    select
        user_id,
        min(starts_at) as first_subscription_starts_at,
        count(distinct subscription_id) as count_of_subscriptions
    from
        {{ ref('stg_bingeflix__subscriptions') }}
    group by 1
),

final as (
    select
        users.user_id,
        users.created_at,
        users.phone_number,
        users.deleted_at,
        users.username,
        users.name,
        users.sex,
        users.email,
        users.birthdate,
        truncate(datediff(month, users.birthdate, current_date() )/12) as current_age,
        truncate(datediff(month, users.birthdate, users.created_at)/12) as age_at_acquisition,
        users.region,
        users.country,
        users_subscription_facts.first_subscription_starts_at,
        users_subscription_facts.count_of_subscriptions
    from
        users
        left join users_subscription_facts
        on users.user_id = users_subscription_facts.user_id
)

select * from final
