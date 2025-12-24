# Lab 03: Creating DRY Macros

## 📋 Overview

| Attribute | Value |
|-----------|-------|
| **Duration** | 30 minutes |
| **Difficulty** | Intermediate |
| **Prerequisites** | Labs 01-02 |

## 🎯 Learning Objectives

- Create reusable macros with parameters
- Use Jinja templating
- Debug macros with `dbt compile`

---

## Exercise: Create a Date Columns Macro

### Step 1: Explore Existing Macros

```bash
ls macros/
cat macros/safe_divide.sql
```

### Step 2: Create the Macro

Create `macros/generate_date_columns.sql`:

```sql
{% macro generate_date_columns(date_column, prefix='date') %}

    extract(year from {{ date_column }}) as {{ prefix }}_year,
    extract(month from {{ date_column }}) as {{ prefix }}_month,
    extract(quarter from {{ date_column }}) as {{ prefix }}_quarter,
    extract(week from {{ date_column }}) as {{ prefix }}_week,
    extract(dayofweek from {{ date_column }}) as {{ prefix }}_day_of_week,
    case 
        when extract(dayofweek from {{ date_column }}) in (1, 7) then true 
        else false 
    end as is_weekend

{% endmacro %}
```

### Step 3: Create a Test Model

Create `models/staging/test_date_macro.sql`:

```sql
{{ config(materialized='view') }}

with sample_dates as (
    select date('2017-01-15') as test_date
    union all
    select date('2017-06-21') as test_date
)

select
    test_date,
    {{ generate_date_columns('test_date', 'test') }}
from sample_dates
```

### Step 4: Compile and Verify

```bash
dbt compile --select test_date_macro
cat target/compiled/dbt_advanced_olist/models/staging/test_date_macro.sql
```

### Step 5: Run the Model

```bash
dbt run --select test_date_macro
dbt show --select test_date_macro
```

### Step 6: Clean Up

```bash
rm models/staging/test_date_macro.sql
```

---

## ✅ Checklist

- [ ] Macro created in `macros/`
- [ ] Compiled SQL looks correct
- [ ] Test model runs successfully
- [ ] Test model deleted

---

## 📚 Key Takeaways

1. **Macros generate SQL** at compile time
2. **Parameters** make macros flexible
3. **Always compile** to check generated SQL

---

## ➡️ Next: Lab 04 - Using dbt Packages
