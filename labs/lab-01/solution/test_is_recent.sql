-- ============================================================
-- LAB 01 SOLUTION: Custom Generic Test - is_recent
-- ============================================================
-- 
-- FILE LOCATION: tests/generic/test_is_recent.sql
--
-- ============================================================

{% test is_recent(model, column_name, days=7) %}
{#
    Test: is_recent
    
    Purpose: Validates that date values are within N days of current date.
             Useful for ensuring data freshness in transactional tables.
    
    Parameters:
        - model: The model being tested (automatically passed by dbt)
        - column_name: The date column to check (automatically passed by dbt)
        - days: Maximum acceptable age in days (default: 7)
    
    Returns: 
        Rows where the date is OLDER than the threshold (failures)
        Zero rows = test passes
        Any rows = test fails
#}

select
    {{ column_name }} as date_value,
    date_diff(current_date(), date({{ column_name }}), day) as days_old,
    count(*) as row_count
from {{ model }}
where {{ column_name }} is not null
  and date_diff(current_date(), date({{ column_name }}), day) > {{ days }}
group by 1, 2

{% endtest %}

-- ============================================================
-- SCHEMA.YML CONFIGURATION
-- ============================================================
--
-- Add to models/marts/_marts.yml under fct_orders:
--
-- columns:
--   - name: order_date
--     description: "Order date (partition key)"
--     data_type: date
--     tests:
--       - not_null
--       - is_recent:
--           days: 3000
--           config:
--             severity: warn
--
-- ============================================================
-- BONUS: Alternative version with error thresholds
-- ============================================================

/*
{% test is_recent_with_threshold(model, column_name, days=7, max_failures=100) %}

with failures as (
    select
        {{ column_name }} as date_value,
        date_diff(current_date(), date({{ column_name }}), day) as days_old
    from {{ model }}
    where {{ column_name }} is not null
      and date_diff(current_date(), date({{ column_name }}), day) > {{ days }}
)

select *
from failures
limit {{ max_failures + 1 }}  -- Return max + 1 to detect if threshold exceeded

{% endtest %}
*/

-- ============================================================
-- BONUS: is_not_future test
-- ============================================================

/*
{% test is_not_future(model, column_name) %}

select
    {{ column_name }} as date_value,
    date_diff(date({{ column_name }}), current_date(), day) as days_in_future
from {{ model }}
where {{ column_name }} is not null
  and date({{ column_name }}) > current_date()

{% endtest %}
*/
