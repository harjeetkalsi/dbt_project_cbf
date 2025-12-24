# Lab 01: Building Custom Generic Tests

## 📋 Overview

| Attribute | Value |
|-----------|-------|
| **Session** | 1 - Building Robust Tests |
| **Duration** | 25 minutes |
| **Difficulty** | Intermediate |
| **Prerequisites** | dbt basics, SQL fundamentals |

## 🎯 Learning Objectives

By the end of this lab, you will be able to:

1. Create a custom generic test macro from scratch
2. Understand the anatomy of dbt test macros (parameters, return values)
3. Apply custom tests to models via schema.yml
4. Configure test severity levels
5. Run and debug tests using dbt CLI

---

## 📖 Background

### What Are Generic Tests?

Generic tests in dbt are **reusable test macros** that can be applied to any model/column via YAML configuration. Unlike singular tests (standalone SQL files), generic tests follow the DRY principle—write once, use everywhere.

**Built-in generic tests:**
- `unique` - No duplicate values
- `not_null` - No NULL values
- `accepted_values` - Values in allowed list
- `relationships` - Foreign key integrity

**Custom generic tests** extend this with your own business logic.

### Test Macro Anatomy

```sql
{% test test_name(model, column_name, optional_param=default) %}

-- Query returns rows that FAIL the test
-- Zero rows = test passes
-- Any rows = test fails

select *
from {{ model }}
where {{ column_name }} violates_some_condition

{% endtest %}
```

---

## 🛠️ Lab Setup

### Step 1: Verify Your Environment

```bash
# Navigate to project directory
cd dbt_advanced_olist

# Activate virtual environment (if using)
source venv/bin/activate

# Verify dbt is working
dbt debug

# Ensure models are built
dbt run
```

### Step 2: Explore Existing Tests

```bash
# View current generic tests
ls tests/generic/

# Run all generic tests
dbt test --select test_type:generic
```

---

## 📝 Exercise: Create `is_recent` Test

### Scenario

Your data team needs to ensure that transactional data is "fresh" - dates shouldn't be too old. Create a reusable test that validates date columns are within N days of today.

### Step 3: Create the Test File

Create a new file `tests/generic/test_is_recent.sql`:

```bash
# Create the file (or use your IDE)
touch tests/generic/test_is_recent.sql
```

### Step 4: Implement the Test

Open `labs/lab-01/starter/test_is_recent.sql` for the template, or start from scratch:

```sql
{% test is_recent(model, column_name, days=7) %}
{#
    Test: is_recent
    
    Purpose: Validates that date values are within N days of current date
    
    Parameters:
        - model: The model being tested (automatically passed)
        - column_name: The date column to check (automatically passed)
        - days: Maximum age in days (default: 7)
    
    Returns: Rows where the date is OLDER than the threshold (failures)
#}

-- YOUR CODE HERE: Write a query that returns rows violating the test
-- Hint 1: Use date_diff(current_date(), date_column, day) for BigQuery
-- Hint 2: Handle NULL values appropriately
-- Hint 3: Return rows that are OLDER than the threshold

select
    {{ column_name }} as date_value,
    date_diff(current_date(), date({{ column_name }}), day) as days_old
from {{ model }}
where {{ column_name }} is not null
  and date_diff(current_date(), date({{ column_name }}), day) > {{ days }}

{% endtest %}
```

### Step 5: Apply the Test to a Model

Edit `models/marts/_marts.yml` to add the test to `fct_orders`:

```yaml
columns:
  - name: order_date
    description: "Order date (partition key)"
    data_type: date
    tests:
      - not_null
      - is_recent:
          days: 3000  # Orders are from 2017, so use large value for demo
          config:
            severity: warn
```

### Step 6: Run Your Test

```bash
# Run only the is_recent test
dbt test --select is_recent

# Run with verbose output to see details
dbt test --select is_recent --log-level debug

# Check compiled SQL
cat target/compiled/dbt_advanced_olist/models/marts/_marts.yml/is_recent_fct_orders_order_date.sql
```

---

## 🔍 Expected Output

### Successful Run (with warnings)

```
15:42:31  Running with dbt=1.7.0
15:42:31  Registered adapter: bigquery=1.7.0
15:42:32  Found 8 models, 42 tests, 0 snapshots, 0 analyses
15:42:33  
15:42:33  Concurrency: 4 threads (target='dev')
15:42:33  
15:42:33  1 of 1 START test is_recent_fct_orders_order_date .......................... [RUN]
15:42:35  1 of 1 WARN 10000 is_recent_fct_orders_order_date .......................... [WARN 10000 in 2.12s]
15:42:35  
15:42:35  Finished running 1 test in 0 hours 0 minutes and 4.23 seconds.
15:42:35  
15:42:35  Completed with 1 warning:
15:42:35  
15:42:35  Warning in test is_recent_fct_orders_order_date
15:42:35    Got 10000 results, configured to warn if >= 1
```

The warning is expected because our 2017 data is older than any reasonable threshold!

---

## 🎯 Bonus Challenges

### Challenge 1: Add Error Thresholds

Modify your test usage to allow some failures:

```yaml
- is_recent:
    days: 365
    config:
      severity: warn
      warn_if: ">100"
      error_if: ">1000"
```

### Challenge 2: Create `is_not_future` Test

Create another generic test that validates dates are not in the future:

```sql
{% test is_not_future(model, column_name) %}

select *
from {{ model }}
where {{ column_name }} > current_date()

{% endtest %}
```

### Challenge 3: Create `is_business_hours` Test

For timestamp columns, test if they fall within business hours (9 AM - 6 PM):

```sql
{% test is_business_hours(model, column_name, start_hour=9, end_hour=18) %}

select *
from {{ model }}
where extract(hour from {{ column_name }}) < {{ start_hour }}
   or extract(hour from {{ column_name }}) > {{ end_hour }}

{% endtest %}
```

---

## ✅ Validation Checklist

Before completing this lab, verify:

- [ ] `test_is_recent.sql` exists in `tests/generic/`
- [ ] Test compiles without errors: `dbt compile --select is_recent`
- [ ] Test runs successfully: `dbt test --select is_recent`
- [ ] You understand why the test returns warnings (data age)
- [ ] You can view the compiled SQL in `target/compiled/`

---

## 📚 Key Takeaways

1. **Generic tests are macros** - They use Jinja templating with `{% test %}` blocks
2. **Tests return failures** - Rows returned = failures; zero rows = pass
3. **Parameters are flexible** - Use default values for optional configs
4. **Severity matters** - Use `warn` for non-blocking issues, `error` for critical ones
5. **Tests are documented in YAML** - Makes them discoverable and self-documenting

---

## 🔗 Resources

- [dbt Testing Documentation](https://docs.getdbt.com/docs/build/tests)
- [Custom Generic Tests](https://docs.getdbt.com/guides/best-practices/writing-custom-generic-tests)
- [Test Configurations](https://docs.getdbt.com/reference/test-configs)

---

## ➡️ Next Lab

Continue to **Lab 02: Implementing Data Contracts** to learn about schema enforcement with dbt 1.5+ contracts.
