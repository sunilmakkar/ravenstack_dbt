with spine as (

    select
        account_id,
        month_start,
        dateadd(day, -1, dateadd(month, 1, month_start))::date as month_end
    from {{ ref('int_account_month_spine') }}

),

subs as (

    select *
    from {{ ref('stg_ravenstack__subscriptions') }}

),

active_subs_in_month as (

    select
        sp.account_id,
        sp.month_start,
        count(distinct s.subscription_id) as active_subscription_count,
        sum(coalesce(s.mrr_amount, 0))    as mrr
    from spine sp
    left join subs s
      on s.account_id = sp.account_id
     and s.start_date <= sp.month_end
     and (s.end_date is null or s.end_date >= sp.month_start)
    group by 1,2

),

churn_events as (

    select
        account_id,
        date_trunc('month', churn_date)::date as month_start,
        min(reason_code) as churn_reason_code
    from {{ ref('stg_ravenstack__churn_events') }}
    group by 1,2

),

base as (

    select
        sp.account_id,
        sp.month_start,

        coalesce(a.mrr, 0) as mrr,
        coalesce(a.active_subscription_count, 0) as active_subscription_count,
        iff(coalesce(a.mrr, 0) > 0, true, false) as is_active,

        coalesce(
            lag(coalesce(a.mrr, 0)) over (
                partition by sp.account_id
                order by sp.month_start
            ),
            0
        ) as prior_month_mrr,

        -- logo churn: churn event in month
        iff(ce.account_id is not null, true, false) as logo_churned,
        ce.churn_reason_code

    from spine sp
    left join active_subs_in_month a
      on a.account_id = sp.account_id
     and a.month_start = sp.month_start
    left join churn_events ce
      on ce.account_id = sp.account_id
     and ce.month_start = sp.month_start

),

final as (

    select
        account_id,
        month_start,

        mrr,
        active_subscription_count,
        is_active,

        prior_month_mrr,

        -- revenue churn: prior > 0 and current = 0
        iff(prior_month_mrr > 0 and mrr = 0, true, false) as mrr_churned,

        logo_churned,
        churn_reason_code,

        -- MRR movement (uses the helper macro)
        case when {{ mrr_waterfall('mrr', 'prior_month_mrr') }} = 'new'
             then mrr else 0 end as new_mrr,

        case when {{ mrr_waterfall('mrr', 'prior_month_mrr') }} = 'expansion'
             then (mrr - prior_month_mrr) else 0 end as expansion_mrr,

        case when {{ mrr_waterfall('mrr', 'prior_month_mrr') }} = 'contraction'
             then (prior_month_mrr - mrr) else 0 end as contraction_mrr,

        case when {{ mrr_waterfall('mrr', 'prior_month_mrr') }} = 'churned'
             then prior_month_mrr else 0 end as churned_mrr

    from base

)

select *
from final

