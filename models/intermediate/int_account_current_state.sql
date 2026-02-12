with subs as (

    select *
    from {{ ref('stg_ravenstack__subscriptions') }}

),

latest_sub as (

    select
        account_id,
        plan_tier            as current_plan_tier,
        billing_frequency    as current_billing_frequency,
        seats                as current_seats,
        mrr_amount           as current_mrr,
        is_trial             as current_is_trial,

        start_date,
        end_date
    from subs
    qualify row_number() over (
        partition by account_id
        order by start_date desc, subscription_id desc
    ) = 1

),

final as (

    select
        account_id,
        current_plan_tier,
        current_billing_frequency,
        current_seats,
        current_mrr,
        current_is_trial,

        iff(
            start_date is not null
            and start_date <= current_date()
            and (end_date is null or end_date >= current_date()),
            true,
            false
        ) as is_active

    from latest_sub

)

select * from final
