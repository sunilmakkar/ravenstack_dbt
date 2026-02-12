with src as (

    select *
    from {{ ref('ravenstack_accounts') }}

),

renamed as (

    select
        cast(account_id as varchar)              as account_id,
        trim(account_name)                      as account_name,
        trim(industry)                          as industry,
        trim(country)                           as country,
        try_to_date(signup_date)                as signup_date,
        lower(trim(referral_source))            as referral_source,
        trim(plan_tier)                         as plan_tier,
        try_to_number(seats)                    as seats,
        try_to_boolean(is_trial)                as is_trial,
        try_to_boolean(churn_flag)              as churn_flag
    from src

)

select *
from renamed
