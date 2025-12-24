# Lab 02: Data Contracts and Unit Tests

## 📋 Overview

| Attribute | Value |
|-----------|-------|
| **Duration** | 30 minutes |
| **Difficulty** | Intermediate |
| **Prerequisites** | Lab 01, dbt 1.8+ |

## 🎯 Learning Objectives

- Implement data contracts to enforce schema
- Write unit tests with mock data
- Understand contract violations

---

## Part A: Data Contracts

### Step 1: Review the Contract

Open `models/marts/_marts.yml` and examine the contract configuration:

```yaml
config:
  contract:
    enforced: true
```

### Step 2: Build with Contract

```bash
dbt build --select dim_customers
```

### Step 3: Break the Contract

Edit `models/marts/dim_customers.sql` and change:

```sql
coalesce(m.lifetime_orders, 0) as lifetime_orders,
```

To:

```sql
cast(coalesce(m.lifetime_orders, 0) as string) as lifetime_orders,
```

Rebuild and observe the error:

```bash
dbt build --select dim_customers
```

**Revert your change after seeing the error.**

---

## Part B: Unit Tests

### Step 4: Run Unit Tests

```bash
dbt test --select test_type:unit
```

### Step 5: Examine a Unit Test

Look at the unit test structure in `_marts.yml`:

```yaml
unit_tests:
  - name: test_customer_segment_vip
    model: dim_customers
    given:
      - input: ref('int_orders_enriched')
        format: dict
        rows:
          - {customer_id: 'c1', order_total: 200.0, ...}
    expect:
      rows:
        - {customer_id: 'c1', customer_segment: 'VIP'}
```

Key concepts:
- `format: dict` defines columns explicitly
- `given` provides mock input data
- `expect` defines expected output

---

## ✅ Checklist

- [ ] Contract error observed when type changed
- [ ] All unit tests pass
- [ ] Changes reverted

---

## 📚 Key Takeaways

1. **Contracts enforce schema** at build time
2. **Unit tests** verify logic with mock data
3. Use `format: dict` to define mock data columns

---

## ➡️ Next: Lab 03 - Creating DRY Macros
