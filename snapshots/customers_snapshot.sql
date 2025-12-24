{% snapshot customers_snapshot %}

{{
    config(
      target_schema='snapshots',
      unique_key='customer_id',
      strategy='check',
      check_cols=['customer_city', 'customer_state', 'customer_zip_code'],
      invalidate_hard_deletes=True
    )
}}

/*
  SNAPSHOT: customers_snapshot
  
  Purpose: Track historical changes to customer data (SCD Type 2)
  
  Strategy: 'check' - monitors specified columns for changes
  
  Tracked columns:
  - customer_city
  - customer_state  
  - customer_zip_code
  
  Added columns:
  - dbt_valid_from: When this version became active
  - dbt_valid_to: When this version was superseded (NULL = current)
  - dbt_scd_id: Unique identifier for this version
  
  Usage:
    dbt snapshot
*/

select
    customer_id,
    customer_unique_id,
    customer_city,
    customer_state,
    customer_zip_code,
    customer_region,
    current_timestamp() as _snapshot_at

from {{ ref('stg_customers') }}

{% endsnapshot %}
