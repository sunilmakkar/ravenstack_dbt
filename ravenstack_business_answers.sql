-- ============================================================================
-- RAVENSTACK_CHURN - BUSINESS ANSWERS (SNOWFLAKE SQL)
-- ============================================================================
-- Purpose: Answer the core business questions:
--          "How is revenue evolving over time, where are we losing customers, and why?"
-- Project: ravenstack_churn (dbt Fusion / dbtf) on Snowflake
-- Data Source: Fully synthetic SaaS dataset (5 CSV seeds -> dbt marts)
-- Primary Mart: fct_account_monthly_subscription (grain: account_id x month_start)
-- Last Updated: February 2026
-- Notes:
--   - These queries are read-only analytics queries (no DDL, no writes).
--   - Assumes the following mart tables exist:
--       dim_date
--       dim_account
--       fct_account_monthly_subscription
-- ============================================================================


-- ============================================================================
-- SECTION 0: CONTEXT / POINTERS (EDIT THESE)
-- ============================================================================
-- Purpose: Centralize the database/schema used by the marts so you can paste+run.
-- Action: Replace ANALYTICS_DB / ANALYTICS_SCHEMA with your actual dbt target.
-- ============================================================================

-- Set the database/schema once and run everything.
use database RAVENSTACK_DBT;
use schema RAVENSTACK_DBT.PUBLIC;
use warehouse RAVENSTACK;

-- ============================================================================
-- SECTION 1: REVENUE OVER TIME (MRR TREND + ACTIVE CUSTOMERS)
-- ============================================================================
-- Purpose: High-level view of revenue evolution month over month.
-- Output:
--   - total_mrr: total monthly recurring revenue in that month
--   - active_accounts: count of accounts marked active in the month
--   - arpa_mrr: avg MRR per active account (simple ARPA)
-- ============================================================================

select
  f.month_start,
  sum(f.mrr)                                  as total_mrr,
  count_if(f.is_active)                        as active_accounts,
  avg(case when f.is_active then f.mrr end)    as arpa_mrr
from RAVENSTACK_DBT.PUBLIC.fct_account_monthly_subscription f
group by 1
order by 1;



-- ============================================================================
-- SECTION 2: REVENUE EVOLUTION EXPLAINED (MRR WATERFALL BY MONTH)
-- ============================================================================
-- Purpose: Explain "why MRR changed" with a monthly waterfall:
--          New + Expansion - Contraction - Churn.
-- Output:
--   - new_mrr, expansion_mrr, contraction_mrr, churned_mrr
--   - ending_mrr: total MRR at end of month
-- Notes:
--   - Assumes your fact model materializes these movement columns.
-- ============================================================================

select
  month_start,
  sum(new_mrr)         as new_mrr,
  sum(expansion_mrr)   as expansion_mrr,
  sum(contraction_mrr) as contraction_mrr,
  sum(churned_mrr)     as churned_mrr,
  sum(mrr)             as ending_mrr
from RAVENSTACK_DBT.PUBLIC.fct_account_monthly_subscription
group by 1
order by 1;



-- ============================================================================
-- SECTION 3: NET MRR CHANGE (MONTH-OVER-MONTH DELTA)
-- ============================================================================
-- Purpose: Show total MRR and MoM change as a simple executive signal.
-- Output:
--   - total_mrr, mrr_delta
-- Notes:
--   - This is often the "headline" chart behind the question.
-- ============================================================================

with m as (
  select
    month_start,
    sum(mrr) as total_mrr
  from RAVENSTACK_DBT.PUBLIC.fct_account_monthly_subscription
  group by 1
)
select
  month_start,
  total_mrr,
  total_mrr - lag(total_mrr) over (order by month_start) as mrr_delta
from m
order by month_start;



-- ============================================================================
-- SECTION 4: WHERE ARE WE LOSING CUSTOMERS? (LOGO CHURN OVER TIME)
-- ============================================================================
-- Purpose: Measure customer churn as "logo churn" (accounts churned in the month).
-- Output:
--   - logo_churned_accounts: count of churned accounts in month
--   - logo_churn_rate: churned / accounts_in_month (as modeled by your spine)
-- Notes:
--   - accounts_in_month counts the rows in the fact (i.e., your month spine coverage).
-- ============================================================================

with base as (
  select
    month_start,
    count(*) as accounts_in_month,
    count_if(logo_churned) as logo_churned_accounts
  from RAVENSTACK_DBT.PUBLIC.fct_account_monthly_subscription
  group by 1
)
select
  month_start,
  accounts_in_month,
  logo_churned_accounts,
  logo_churned_accounts / nullif(accounts_in_month, 0) as logo_churn_rate
from base
order by month_start;



-- ============================================================================
-- SECTION 5: WHERE ARE WE LOSING CUSTOMERS? (LOGO CHURN BY SEGMENT)
-- ============================================================================
-- Purpose: Identify churn concentration by segment (e.g., plan tier).
-- Output:
--   - logo_churn_rate by month x segment
-- How to use:
--   - Swap a.initial_plan_tier with a.industry / a.country / a.tenure_bucket.
-- ============================================================================

select
  f.month_start,
  a.initial_plan_tier,
  count(*) as accounts_in_month,
  count_if(f.logo_churned) as logo_churned_accounts,
  count_if(f.logo_churned) / nullif(count(*), 0) as logo_churn_rate
from RAVENSTACK_DBT.PUBLIC.fct_account_monthly_subscription f
join RAVENSTACK_DBT.PUBLIC.dim_account a
  on f.account_id = a.account_id
group by 1, 2
order by 1, 2;

-- ============================================================================
-- SECTION 6: WHERE ARE WE LOSING REVENUE? (REVENUE CHURN OVER TIME)
-- ============================================================================
-- Purpose: Quantify revenue churn (lost MRR from previously-paying accounts).
-- Output:
--   - churned_mrr_from_prior: sum(prior_month_mrr) for accounts that churned revenue
--   - revenue_churn_rate: churned_mrr_from_prior / prior_mrr
-- Notes:
--   - This aligns to your definition: prior_month_mrr > 0 AND current mrr = 0.
-- ============================================================================

with m as (
  select
    month_start,
    sum(mrr) as ending_mrr,
    sum(prior_month_mrr) as prior_mrr,
    sum(case when mrr_churned then prior_month_mrr else 0 end) as churned_mrr_from_prior
  from RAVENSTACK_DBT.PUBLIC.fct_account_monthly_subscription
  group by 1
)
select
  month_start,
  prior_mrr,
  churned_mrr_from_prior,
  churned_mrr_from_prior / nullif(prior_mrr, 0) as revenue_churn_rate,
  ending_mrr
from m
order by month_start;



-- ============================================================================
-- SECTION 7: WHERE ARE WE LOSING REVENUE? (REVENUE CHURN BY SEGMENT)
-- ============================================================================
-- Purpose: Identify which segments drive revenue loss.
-- Output:
--   - churned_mrr and revenue_churn_rate by month x segment
-- How to use:
--   - Swap a.initial_plan_tier with a.industry / a.country / a.tenure_bucket.
-- ============================================================================

select
  f.month_start,
  a.initial_plan_tier,
  sum(case when f.mrr_churned then f.prior_month_mrr else 0 end) as churned_mrr,
  sum(f.prior_month_mrr) as prior_mrr,
  sum(case when f.mrr_churned then f.prior_month_mrr else 0 end) / nullif(sum(f.prior_month_mrr), 0) as revenue_churn_rate
from RAVENSTACK_DBT.PUBLIC.fct_account_monthly_subscription f
join RAVENSTACK_DBT.PUBLIC.dim_account a
  on f.account_id = a.account_id
group by 1, 2
order by 1, 2;



-- ============================================================================
-- SECTION 8: WHY ARE WE LOSING CUSTOMERS? (TOP CHURN REASONS OVER TIME)
-- ============================================================================
-- Purpose: Break down churn by reason code each month (counts).
-- Output:
--   - churned_accounts per month x churn_reason_code
-- Notes:
--   - Uses logo_churned to align "why" with churn events.
-- ============================================================================

select
  month_start,
  churn_reason_code,
  count_if(logo_churned) as churned_accounts
from RAVENSTACK_DBT.PUBLIC.fct_account_monthly_subscription
where logo_churned
group by 1, 2
order by 1, churned_accounts desc;



-- ============================================================================
-- SECTION 9: WHY ARE WE LOSING CUSTOMERS? (CHURN REASONS BY SEGMENT)
-- ============================================================================
-- Purpose: Find which segments churn for which reasons (e.g., industry x reason).
-- Output:
--   - churned_accounts per month x segment x churn_reason_code
-- How to use:
--   - Swap a.industry with a.initial_plan_tier / a.country / a.tenure_bucket.
-- ============================================================================

select
  f.month_start,
  a.industry,
  f.churn_reason_code,
  count(*) as churned_accounts
from RAVENSTACK_DBT.PUBLIC.fct_account_monthly_subscription f
join RAVENSTACK_DBT.PUBLIC.dim_account a
  on f.account_id = a.account_id
where f.logo_churned
group by 1, 2, 3
order by 1, 2, churned_accounts desc;



-- ============================================================================
-- SECTION 10: WHY + $ IMPACT (REASON -> CHURNED MRR)
-- ============================================================================
-- Purpose: Tie churn reasons to revenue loss (not just account counts).
-- Output:
--   - churned_mrr by month x churn_reason_code
-- Notes:
--   - Uses revenue churn definition (mrr_churned) and attributes loss to reason.
-- ============================================================================

select
  f.month_start,
  f.churn_reason_code,
  sum(case when f.mrr_churned then f.prior_month_mrr else 0 end) as churned_mrr
from RAVENSTACK_DBT.PUBLIC.fct_account_monthly_subscription f
where f.logo_churned or f.mrr_churned
group by 1, 2
order by 1, churned_mrr desc;



-- ============================================================================
-- SECTION 11: EXEC SUMMARY (ONE TABLE ANSWERS THE WHOLE QUESTION)
-- ============================================================================
-- Purpose: Single monthly panel:
--   - ending MRR + MoM delta
--   - MRR waterfall components
--   - logo churn count
--   - top churn reason (by churned accounts)
-- Output:
--   - one row per month
-- ============================================================================

with monthly as (
  select
    month_start,
    sum(mrr) as ending_mrr,
    sum(new_mrr) as new_mrr,
    sum(expansion_mrr) as expansion_mrr,
    sum(contraction_mrr) as contraction_mrr,
    sum(churned_mrr) as churned_mrr,
    count_if(logo_churned) as logo_churned_accounts
  from RAVENSTACK_DBT.PUBLIC.fct_account_monthly_subscription
  group by 1
),
top_reason as (
  select
    month_start,
    churn_reason_code,
    count(*) as churned_accounts,
    row_number() over (partition by month_start order by count(*) desc) as rn
  from RAVENSTACK_DBT.PUBLIC.fct_account_monthly_subscription
  where logo_churned
  group by 1, 2
)
select
  m.month_start,
  m.ending_mrr,
  m.ending_mrr - lag(m.ending_mrr) over (order by m.month_start) as mrr_delta,
  m.new_mrr,
  m.expansion_mrr,
  m.contraction_mrr,
  m.churned_mrr,
  m.logo_churned_accounts,
  r.churn_reason_code as top_churn_reason
from monthly m
left join top_reason r
  on m.month_start = r.month_start
 and r.rn = 1
order by m.month_start;

-- ============================================================================
-- BUSINESS SUMMARY — "How is revenue evolving, where are we losing customers,
-- and why?"
-- ============================================================================
--
-- Revenue shows strong, sustained growth from Jan-2023 through Dec-2024.
-- Ending MRR grows from ~4.7K in Jan-2023 to ~10.7M by Dec-2024, driven primarily
-- by expansion revenue rather than new customer acquisition.
--
-- Across most months in 2024, expansion MRR materially exceeds new MRR,
-- indicating that upsells, seat growth, and plan upgrades are the dominant
-- growth engine rather than logo growth alone.
--
-- Customer churn (logo churn) steadily increases over time, rising from low
-- single-digit counts in early 2023 to 60–96 churned accounts per month by
-- late 2024. The most common churn drivers in recent months are:
--   - features
--   - competitor
--   - budget
-- and occasionally pricing, suggesting product capability and competitive
-- pressure are the primary retention risks.
--
-- Despite rising logo churn, revenue churn remains very low throughout most
-- of the growth period. This indicates that the customers who churn tend to be
-- smaller accounts, while larger accounts continue to expand and retain.
--
-- A clear inflection occurs in Jan-2025, where total MRR drops by ~575K and all
-- revenue movement is classified as contraction. From that point onward,
-- revenue becomes flat and no further new, expansion, churn, or contraction
-- activity is observed.
--
-- This plateau strongly suggests that the underlying synthetic dataset ends
-- around late-2024 and the month spine continues generating future months with
-- no new subscription or churn activity. As a result, months after Jan-2025
-- should be interpreted as out-of-data months rather than true business
-- behavior.
--
-- In summary:
-- - Revenue growth is primarily expansion-led.
-- - Customer losses are increasingly driven by product features, competitors,
--   and budget constraints.
-- - Churned revenue is minimal relative to total MRR during the growth period,
--   implying churn is concentrated among lower-value customers.
-- - The post-2024 flat period reflects data coverage limits rather than an
--   operational slowdown.
-- ============================================================================
