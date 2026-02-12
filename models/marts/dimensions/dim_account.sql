with accounts as (

    select *
    from {{ ref('stg_ravenstack__accounts') }}

),

max_date as (

    select max(date_day) as as_of_date
    from {{ ref('dim_date') }}

),

final as (

    select
        a.account_id,
        a.account_name,
        a.industry,
        a.country,
        a.signup_date,
        a.referral_source,

        a.plan_tier                 as initial_plan_tier,
        a.seats                     as initial_seats,
        a.is_trial                  as initial_is_trial,

        a.churn_flag                as churn_flag_ever,

        datediff('day', a.signup_date, (select as_of_date from max_date)) as tenure_days,
        {{ tenure_bucket('tenure_days') }} as tenure_bucket

    from accounts a

)

select *
from final
