-- If mrr_churned = true, then mrr must be 0 and prior_month_mrr must be > 0
select *
from {{ ref('fct_account_monthly_subscription') }}
where mrr_churned = true
  and not (mrr = 0 and prior_month_mrr > 0)
