with accounts as (

    select
        account_id,
        date_trunc('month', signup_date)::date as signup_month
    from {{ ref('stg_ravenstack__accounts') }}

),

date_months as (

    select distinct
        month_start
    from {{ ref('dim_date') }}

),

spine as (

    select
        a.account_id,
        d.month_start
    from accounts a
    join date_months d
      on d.month_start >= a.signup_month

)

select *
from spine
