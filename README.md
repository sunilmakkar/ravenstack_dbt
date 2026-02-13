# RavenStack – SaaS Subscription & Churn Analytics (dbt project)

This project models a synthetic SaaS business to answer a common leadership question:

**"How is revenue evolving over time, where are we losing customers, and why?"**

The dataset represents accounts, subscriptions, product usage, support activity, and recorded churn events.

The goal of the project is not to build a large analytics platform, but to demonstrate clear and correct analytics engineering practices for subscription and churn reporting in a small project that can be reviewed in a few minutes.

**Dataset credit:**  
This project uses a fully synthetic SaaS dataset created by **River @ Rivalytics**.

**Read my blog post to learn more:** 
https://medium.com/@sunil.makkar97/how-i-answered-a-real-saas-churn-question-with-one-clean-dbt-model-d02164d2de3f

---

## Executive summary – what the data shows

Using the models in this project, we produce a monthly account-level fact table and answer the business question directly.

At a high level, the data shows that:

- Revenue grows strongly and consistently from early 2023 through the end of 2024.
- Growth is primarily driven by **expansion revenue** (existing customers upgrading or expanding usage), rather than new customer acquisition alone.
- **Logo churn increases steadily over time**, especially in late 2024.
- The most common churn drivers are:
  - product features
  - competitors
  - budget constraints
  - and, less frequently, pricing
- Despite increasing logo churn, **revenue churn remains relatively small**, indicating that churn is concentrated among smaller accounts while larger customers continue to expand.
- After early 2025, revenue becomes flat. This reflects the end of the synthetic source data rather than a modeled business slowdown, while the account-month spine continues generating future reporting periods.

This project is designed to show how these conclusions can be derived reliably and reproducibly from well-modeled analytics tables.

---

## What this project enables

This project produces a clean monthly view of every account that allows analysts and stakeholders to:

- track monthly recurring revenue (MRR) by account and plan
- understand how many subscriptions are active each month
- distinguish between logo churn and revenue churn
- analyze how revenue changes over time (new, expansion, contraction, churned MRR)
- segment churn by plan tier, tenure, industry, and churn reason

Typical business questions this model supports include:

- Which plan tiers churn the most?
- Do newer customers churn at higher rates than long-tenured customers?
- Are we losing revenue primarily through contraction or full churn?
- What are the most common churn reasons by industry?
- Is revenue growth being driven by new customers or existing customer expansion?

---

## Modeling approach

The project follows a standard analytics engineering layering pattern.

The design intentionally prioritizes:
- clear grains,
- auditable business logic,
- and small, composable models.

### Raw data (seeds)

All source data is loaded as dbt seeds:

- `accounts`
- `subscriptions`
- `feature_usage`
- `support_tickets`
- `churn_events`

These represent operational system extracts for a fictional SaaS business.

---

### Staging layer

The staging layer provides thin, cleaned versions of each source table.

Staging models only perform:

- column renaming
- light type casting
- basic cleanup

No business logic is introduced at this layer.

The staging models act as a stable contract for downstream models.

---

### Intermediate layer

Two intermediate models support the core business logic.

#### Account–month spine

A complete account × calendar month spine is generated starting from each account's signup month.

This guarantees that:

- months with zero revenue are still represented
- churn and reactivation behavior can be analyzed cleanly
- revenue trends are not distorted by missing time periods

#### Current account subscription state

A one-row-per-account model identifies the most recent subscription record and represents the current operational state of the account.

This model is used both for reporting and for snapshotting.

---

### Mart layer

The mart layer contains three business-ready models.

#### Date dimension

A daily calendar dimension that supports consistent month boundaries and time-based reporting.

#### Account dimension

A single row per account containing descriptive attributes and derived tenure fields, including:

- initial plan and seat configuration
- whether the account has ever churned
- tenure in days and tenure bucket

#### Monthly subscription fact

The central fact table has a grain of:

**one row per account per calendar month**

This table contains:

- monthly MRR
- number of active subscriptions
- active account flag
- prior month MRR
- revenue churn and logo churn flags
- churn reason code
- MRR movement fields:
  - new MRR
  - expansion MRR
  - contraction MRR
  - churned MRR

This model is designed to be directly consumable by BI tools and ad-hoc analysis and is the primary source used to answer the business question.

---

## Grain definitions

### `dim_account`
One row per account.

### `dim_date`
One row per calendar day.

### `fct_account_monthly_subscription`
One row per account per calendar month (`month_start`).

---

## Business definitions

### Active subscription in a month

A subscription is considered active for a given month when:

```
subscription.start_date <= month_end
AND
(subscription.end_date IS NULL OR subscription.end_date >= month_start)
```

### Monthly MRR

Monthly recurring revenue is calculated as the sum of `mrr_amount` for all subscriptions active during the month.

### Logo churn

An account is considered logo-churned in a month when a churn event exists whose `churn_date` falls within that month.

### Revenue churn

An account is considered revenue-churned in a month when:

```
prior_month_mrr > 0
AND
current_month_mrr = 0
```

### MRR movement classification

Monthly MRR movement is classified into:

- new
- expansion
- contraction
- churned

using a dedicated macro to keep the logic consistent and auditable.

---

## Snapshotting strategy

The project includes a snapshot of the current subscription state per account.

The snapshot tracks changes to:

- plan tier
- billing frequency
- seat count
- MRR
- trial status
- active flag

This enables historical analysis of how account subscription states evolve over time without re-deriving past states from event data.

The snapshot uses a check-based strategy to capture only meaningful operational changes.

---

## Testing strategy

The project includes realistic, targeted tests focused on business correctness:

- primary key tests on all staging models
- relationship tests for all foreign keys
- accepted values tests for controlled business fields
- fact grain validation (`account_id`, `month_start`)
- non-negative MRR enforcement
- a custom integrity test ensuring revenue churn logic is correct

The intent is to validate both data quality and business rules without over-testing.

---

## Why these modeling choices were made

### Account-month spine

Creating an explicit account-month spine ensures that:

- zero-revenue months are visible
- churn and reactivation behavior can be analyzed cleanly
- time series reporting remains consistent and gap-free

This is essential for reliable churn and revenue trend analysis.

### Separate current state model

Separating current subscription state from historical facts allows:

- clean snapshotting
- simpler operational reporting
- a clear separation between point-in-time state and monthly performance

### Explicit churn and MRR movement logic

Churn and MRR movement logic is implemented directly in the fact model rather than being left to downstream tools, ensuring:

- consistent business definitions
- reproducibility across analyses
- auditability of revenue reporting

---

## Analysis examples

The project includes example analysis queries demonstrating how the mart layer is consumed.

### Churn rates by plan tier and tenure bucket

```sql
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
```

### Churn reasons by industry

```sql
select
  d.industry,
  f.churn_reason_code,
  count(*) as churn_events
from {{ ref('fct_account_monthly_subscription') }} f
join {{ ref('dim_account') }} d using (account_id)
where f.logo_churned = true
group by 1,2
order by churn_events desc;
```

These queries demonstrate how the models support the business question directly.

---

## How to run

This project is designed to be run using dbt Fusion with Snowflake.

Main commands:

```bash
dbtf seed
dbtf build
```

All models, tests, and snapshots should complete successfully with:

```bash
dbtf build
```
