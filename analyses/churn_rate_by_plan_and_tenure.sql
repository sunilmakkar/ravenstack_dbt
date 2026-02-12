with base as (
  select
    f.month_start,
    d.initial_plan_tier,
    d.tenure_bucket,
    f.is_active,
    f.mrr_churned,
    f.logo_churned
  from {{ ref('fct_account_monthly_subscription') }} f
  join {{ ref('dim_account') }} d using (account_id)
)
select
  month_start,
  initial_plan_tier,
  tenure_bucket,
  count(*) as account_months,
  sum(case when is_active then 1 else 0 end) as active_account_months,
  avg(case when logo_churned then 1 else 0 end) as logo_churn_rate,
  avg(case when mrr_churned then 1 else 0 end)  as revenue_churn_rate
from base
group by 1,2,3
order by 1,2,3;
