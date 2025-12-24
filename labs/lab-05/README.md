# Lab 05: Setting Up Slim CI

## 📋 Overview

| Attribute | Value |
|-----------|-------|
| **Duration** | 30 minutes |
| **Difficulty** | Advanced |
| **Prerequisites** | Labs 01-04 completed |

## 🎯 Learning Objectives

- Understand Slim CI concepts
- Use state selectors
- Build only changed models

---

## Background

### What Is Slim CI?

Slim CI builds only changed models instead of the entire project:

```bash
# Full build (slow)
dbt build

# Slim CI (fast)
dbt build --select state:modified+ --state prod-artifacts/
```

### State Selectors

| Selector | Description |
|----------|-------------|
| `state:modified` | Changed models only |
| `state:modified+` | Changed + downstream |
| `state:new` | New models only |

---

## Exercise

### Step 1: Build All Models

```bash
dbt build
ls target/manifest.json
```

### Step 2: Create Baseline

```bash
mkdir -p prod-artifacts
cp target/manifest.json prod-artifacts/manifest.json
```

### Step 3: Make a Change

Edit `models/staging/stg_orders.sql` and add:

```sql
case
    when order_status = 'delivered' then 'Completed'
    when order_status = 'canceled' then 'Canceled'
    else 'In Progress'
end as order_status_group,
```

### Step 4: List Modified Models

```bash
dbt ls --select state:modified+ --state prod-artifacts/
```

Expected output:
```
staging.stg_orders
intermediate.int_orders_enriched
marts.fct_orders
marts.dim_customers
```

### Step 5: Run Slim CI

```bash
dbt build --select state:modified+ --state prod-artifacts/
```

### Step 6: Revert Changes

```bash
git checkout models/staging/stg_orders.sql
```

---

## ✅ Checklist

- [ ] Baseline manifest created
- [ ] state:modified+ identifies changes
- [ ] Slim CI build completes

---

## 📚 Key Takeaways

1. **Slim CI** builds only what changed
2. **Manifest** provides state comparison
3. **state:modified+** includes downstream

---

## ➡️ Next: Lab 06 - Analyzing Pipeline Performance
