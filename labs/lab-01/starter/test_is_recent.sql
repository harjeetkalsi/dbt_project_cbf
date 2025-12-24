-- ============================================================
-- LAB 01 STARTER: Custom Generic Test - is_recent
-- ============================================================
-- 
-- OBJECTIVE: Create a reusable test that checks if date values
--            are within N days of the current date
--
-- FILE LOCATION: Save this as tests/generic/test_is_recent.sql
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
    
    Usage in schema.yml:
        columns:
          - name: order_date
            tests:
              - is_recent:
                  days: 30
                  config:
                    severity: warn
    
    Returns: 
        Rows where the date is OLDER than the threshold (these are failures)
        Zero rows = test passes
        Any rows = test fails
#}

-- ============================================================
-- TODO: Complete the test query below
-- ============================================================
-- 
-- Requirements:
-- 1. Select rows where the date column is older than 'days' threshold
-- 2. Handle NULL values (don't count NULLs as failures)
-- 3. Return useful columns for debugging (the date value, how old it is)
--
-- Hints:
-- - BigQuery syntax: date_diff(current_date(), date_column, day)
-- - Snowflake syntax: datediff(day, date_column, current_date())
-- - Cast to date if the column is a timestamp: date({{ column_name }})
-- - Filter out NULLs: where {{ column_name }} is not null
--
-- ============================================================

select
    -- TODO: Select the date column being tested
    -- {{ column_name }} as date_value,
    
    -- TODO: Calculate the age in days
    -- date_diff(current_date(), date({{ column_name }}), day) as days_old,
    
    -- TODO: Include a count for summary (optional but helpful)
    -- count(*) as row_count
    
    *
from {{ model }}
where 
    -- TODO: Add your filter conditions
    -- 1. Column is not null
    -- 2. Date is older than threshold
    1 = 0  -- Remove this placeholder and add your conditions

{% endtest %}

-- ============================================================
-- TESTING YOUR IMPLEMENTATION
-- ============================================================
--
-- Step 1: Save this file to tests/generic/test_is_recent.sql
--
-- Step 2: Add the test to a model in schema.yml:
--
--   models:
--     - name: fct_orders
--       columns:
--         - name: order_date
--           tests:
--             - is_recent:
--                 days: 3000  # Large value since data is from 2017
--                 config:
--                   severity: warn
--
-- Step 3: Run the test:
--   dbt test --select is_recent
--
-- Step 4: Check the compiled SQL:
--   cat target/compiled/dbt_advanced_olist/models/marts/_marts.yml/is_recent_*.sql
--
-- ============================================================
