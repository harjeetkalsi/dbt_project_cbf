-- ============================================================
-- ANALYSIS: Compare Model Versions with Audit Helper
-- ============================================================
-- 
-- Use this analysis to compare two versions of a model.
-- Great for validating refactoring or debugging issues.
--
-- Run: dbt compile --select compare_customers
-- Then copy the compiled SQL to BigQuery
-- ============================================================

{% set current = ref('dim_customers') %}

-- Compare current vs itself (in real use, compare to old version)
{{ audit_helper.compare_relations(
    a_relation=ref('dim_customers'),
    b_relation=ref('dim_customers'),
    primary_key='customer_id',
    exclude_columns=['_loaded_at']
) }}
```

-- ============================================================
-- INTERPRETATION:
-- 
-- in_a | in_b | count | meaning
-- -----|------|-------|--------
-- true | true | 9000  | Matching rows (good!)
-- true | false| 100   | Rows only in A (deleted in B?)
-- false| true | 50    | Rows only in B (new in B?)
--
-- ============================================================
