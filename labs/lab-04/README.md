# Lab 04: Using dbt Packages

## 📋 Overview

| Attribute | Value |
|-----------|-------|
| **Duration** | 30 minutes |
| **Difficulty** | Intermediate |
| **Prerequisites** | Labs 01-03 |

## 🎯 Learning Objectives

- Install dbt packages
- Understand dbt_utils.date_spine
- Use dbt_audit_helper to compare models

---

## Setup

### Step 1: Install Packages

```bash
cat packages.yml
dbt deps
ls dbt_packages/
```

---

## Part A: Explore Date Dimension

### Step 2: Review dim_date Model

The project includes a pre-built date dimension using `dbt_utils.date_spine`.

Open and review `models/marts/dim_date.sql`:

```sql
{{ config(materialized='table') }}

with date_spine as (
    {{ dbt_utils.date_spine(
        datepart="day",
        start_date="cast('2016-01-01' as date)",
        end_date="cast('2019-01-01' as date)"
    ) }}
),

final as (
    select
        cast(date_day as date) as date_key,
        extract(year from date_day) as year,
        extract(month from date_day) as month,
        extract(quarter from date_day) as quarter,
        extract(week from date_day) as week_of_year,
        extract(dayofweek from date_day) as day_of_week,
        format_date('%A', date_day) as day_name,
        format_date('%B', date_day) as month_name,
        case 
            when extract(dayofweek from date_day) in (1, 7) then true 
            else false 
        end as is_weekend
    from date_spine
)

select * from final
```

### Step 3: Build and Verify

```bash
dbt run --select dim_date
dbt show --select dim_date --limit 10
```

**Discussion Questions:**
- What does `date_spine` generate?
- How would you add a fiscal year column?
- Why is this materialized as a `table` not a `view`?

---

## Part B: Audit Helper (Comparing Model Changes)

The `dbt_audit_helper` package helps compare two versions of a model to see what changed.

### Step 4: Create a Temporary Baseline

Create a **temporary** file `models/marts/dim_customers_baseline.sql`:

```sql
{{ config(materialized='table') }}
select * from {{ ref('dim_customers') }}
```

Build it:
```bash
dbt run --select dim_customers_baseline
```

> ⚠️ **Note:** This is a temporary model for comparison. We will delete it at the end of this lab.

### Step 5: Make a Change to dim_customers

Edit `models/marts/dim_customers.sql` - find the VIP segment threshold and change from 500 to 400:

```sql
-- Change this line:
when lifetime_revenue >= 500 and lifetime_orders >= 3 then 'VIP'

-- To this:
when lifetime_revenue >= 400 and lifetime_orders >= 3 then 'VIP'
```

Rebuild the changed model:
```bash
dbt run --select dim_customers
```

### Step 6: Compare Using Audit Helper

First, update `analyses/compare_customers.sql` to compare baseline vs current:

```sql
-- ============================================================
-- ANALYSIS: Compare Model Versions with Audit Helper
-- ============================================================

{{ audit_helper.compare_relations(
    a_relation=ref('dim_customers_baseline'),
    b_relation=ref('dim_customers'),
    primary_key='customer_id',
    exclude_columns=['_loaded_at']
) }}
```

Then compile and run:

```bash
dbt compile --select compare_customers
```

View the compiled SQL in `target/compiled/dbt_advanced_olist/analyses/compare_customers.sql` and run it in BigQuery to see the differences.

### Step 7: Clean Up (Important!)

**Revert ALL your changes:**

```bash
# 1. Revert the VIP threshold change in dim_customers.sql (change 400 back to 500)

# 2. Revert compare_customers.sql to compare dim_customers to itself:
#    Change: a_relation=ref('dim_customers_baseline')
#    Back to: a_relation=ref('dim_customers')
#    And: b_relation=ref('dim_customers')

# 3. Delete the temporary baseline model
rm models/marts/dim_customers_baseline.sql

# 4. Rebuild
dbt run --select dim_customers
```

> 💡 **Best Practice:** Always clean up temporary models to keep your project organized.

---

## ✅ Checklist

- [ ] dim_date exists and builds successfully
- [ ] Understand how date_spine generates dates
- [ ] Created temporary baseline for comparison
- [ ] Modified compare_customers.sql for comparison
- [ ] Ran audit_helper and saw differences
- [ ] **Reverted dim_customers.sql (VIP threshold back to 500)**
- [ ] **Reverted compare_customers.sql**
- [ ] **Deleted dim_customers_baseline.sql**

---

## 📚 Key Takeaways

1. **date_spine** generates complete date ranges - no gaps
2. **audit_helper** compares model outputs to find differences
3. Always clean up temporary comparison models
4. Packages extend dbt with reusable functionality

---

## ➡️ Next: Lab 05 - Setting Up Slim CI
