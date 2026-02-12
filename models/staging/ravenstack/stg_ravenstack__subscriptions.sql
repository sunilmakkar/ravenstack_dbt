with src as (

    select *
    from {{ ref('ravenstack_subscriptions') }}

),

renamed as (

    select
        cast(subscription_id as varchar)     as subscription_id,
        cast(account_id as varchar)          as account_id,

        try_to_date(start_date)              as start_date,
        try_to_date(end_date)                as end_date,

        trim(plan_tier)                      as plan_tier,
        try_to_number(seats)                as seats,

        try_to_number(mrr_amount)            as mrr_amount,
        try_to_number(arr_amount)            as arr_amount,

        try_to_boolean(is_trial)             as is_trial,
        try_to_boolean(upgrade_flag)         as upgrade_flag,
        try_to_boolean(downgrade_flag)       as downgrade_flag,
        try_to_boolean(churn_flag)           as churn_flag,

        lower(trim(billing_frequency))       as billing_frequency,
        try_to_boolean(auto_renew_flag)      as auto_renew_flag

    from src

)

select *
from renamed
