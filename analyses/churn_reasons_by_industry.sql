select
  d.industry,
  f.churn_reason_code,
  count(*) as churn_events
from {{ ref('fct_account_monthly_subscription') }} f
join {{ ref('dim_account') }} d using (account_id)
where f.logo_churned = true
group by 1,2
order by churn_events desc;
