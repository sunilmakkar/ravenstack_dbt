with bounds as (

    select
        min(d)::date as min_date,
        max(d)::date as max_date
    from (
        select min(signup_date) as d from {{ ref('stg_ravenstack__accounts') }}
        union all select max(signup_date) from {{ ref('stg_ravenstack__accounts') }}
        union all select min(start_date)  from {{ ref('stg_ravenstack__subscriptions') }}
        union all select max(coalesce(end_date, current_date())) from {{ ref('stg_ravenstack__subscriptions') }}
        union all select min(usage_date)  from {{ ref('stg_ravenstack__feature_usage') }}
        union all select max(usage_date)  from {{ ref('stg_ravenstack__feature_usage') }}
        union all select min(churn_date)  from {{ ref('stg_ravenstack__churn_events') }}
        union all select max(churn_date)  from {{ ref('stg_ravenstack__churn_events') }}
        union all select min(cast(submitted_at as date)) from {{ ref('stg_ravenstack__support_tickets') }}
        union all select max(cast(coalesce(closed_at, submitted_at) as date)) from {{ ref('stg_ravenstack__support_tickets') }}
    ) x
    where d is not null

),

calendar as (

    select
        dateadd(day, seq4(), b.min_date)::date as date_day
    from bounds b,
         table(generator(rowcount => 50000)) -- safe upper bound
    qualify date_day <= (select max_date from bounds)

)

select
    date_day,
    date_trunc('month', date_day)::date                               as month_start,
    last_day(date_day, 'month')::date                                  as month_end,

    year(date_day)                                                     as year,
    quarter(date_day)                                                  as quarter,
    month(date_day)                                                    as month,

    dayofweekiso(date_day)                                             as day_of_week,
    iff(dayofweekiso(date_day) in (6,7), true, false)                   as is_weekend

from calendar
