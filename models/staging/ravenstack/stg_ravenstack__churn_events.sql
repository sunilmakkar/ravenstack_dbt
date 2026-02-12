with src as (

    select *
    from {{ ref('ravenstack_churn_events') }}

),

renamed as (

    select
        cast(churn_event_id as varchar)        as churn_event_id,
        cast(account_id as varchar)            as account_id,

        try_to_date(churn_date)                as churn_date,

        lower(trim(reason_code))               as reason_code,

        cast(refund_amount_usd as number(38,2)) as refund_amount_usd,

        try_to_boolean(preceding_upgrade_flag)
                                              as preceding_upgrade_flag,
        try_to_boolean(preceding_downgrade_flag)
                                              as preceding_downgrade_flag,
        try_to_boolean(is_reactivation)        as is_reactivation,

        nullif(trim(feedback_text), '')        as feedback_text

    from src

)

select *
from renamed
