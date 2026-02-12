{% snapshot snap_account_subscription_state %}

{{
  config(
    target_schema='snapshots',
    unique_key='account_id',
    strategy='check',
    check_cols=[
      'current_plan_tier',
      'current_billing_frequency',
      'current_seats',
      'current_mrr',
      'current_is_trial',
      'is_active'
    ]
  )
}}

select
  account_id,
  current_plan_tier,
  current_billing_frequency,
  current_seats,
  current_mrr,
  current_is_trial,
  is_active
from {{ ref('int_account_current_state') }}

{% endsnapshot %}
