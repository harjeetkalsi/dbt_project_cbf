# Lab 06: Analyzing Pipeline Performance

## 📋 Overview

| Attribute | Value |
|-----------|-------|
| **Session** | 3 - CI/CD & Production |
| **Duration** | 25 minutes |
| **Difficulty** | Intermediate |
| **Prerequisites** | Labs 01-05, Python basics |

## 🎯 Learning Objectives

By the end of this lab, you will be able to:

1. Parse and analyze dbt artifacts (run_results.json)
2. Identify slow models and performance bottlenecks
3. Understand the critical path in your DAG
4. Implement optimizations (partitioning, clustering, materialization)
5. Measure improvement after optimization

---

## 📖 Background

### dbt Artifacts

dbt generates several JSON files in the `target/` directory after each run:

| File | Purpose |
|------|---------|
| `manifest.json` | Complete project graph, node configs |
| `run_results.json` | Execution timing, status, errors |
| `catalog.json` | Column metadata (after `dbt docs generate`) |
| `sources.json` | Source freshness results |

### run_results.json Structure

```json
{
  "metadata": { "dbt_version": "1.7.0", ... },
  "results": [
    {
      "unique_id": "model.project.model_name",
      "status": "success",
      "execution_time": 5.234,
      "timing": [
        {"name": "compile", "started_at": "...", "completed_at": "..."},
        {"name": "execute", "started_at": "...", "completed_at": "..."}
      ]
    }
  ]
}
```

### Performance Bottlenecks

Common causes of slow models:

| Issue | Symptom | Solution |
|-------|---------|----------|
| Full table scans | High execute time | Add partitioning |
| Missing clustering | Slow filters/joins | Add cluster_by |
| Wrong materialization | Rebuilding large tables | Use incremental |
| Complex subqueries | High compile time | Break into CTEs/models |
| Cartesian joins | Massive row counts | Fix join conditions |

---

## 🛠️ Lab Setup

### Step 1: Generate Run Results

```bash
# Run a full build to generate artifacts
dbt build

# Verify artifacts exist
ls -la target/run_results.json
ls -la target/manifest.json
```

### Step 2: Review Python Script

View the analysis script in `labs/lab-06/starter/analyze_results.py`:

```bash
cat labs/lab-06/starter/analyze_results.py
```

---

## 📝 Part A: Analyze Run Results

### Step 3: Run the Analysis Script

```bash
# Run the provided Python script
python labs/lab-06/starter/analyze_results.py
```

**Expected Output:**
```
============================================================
DBT PERFORMANCE REPORT
============================================================

Model                                    Status     Time (s)  
------------------------------------------------------------
fct_orders                               success    8.45      
dim_customers                            success    6.23      
int_orders_enriched                      success    4.12      
int_products_enriched                    success    2.34      
stg_orders                               success    1.89      
stg_order_items                          success    1.45      
stg_customers                            success    1.23      
stg_products                             success    0.98      
stg_product_categories                   success    0.45      

------------------------------------------------------------
Total models: 9
Successful: 9
Errors: 0
Total time: 27.14 seconds
============================================================
```

### Step 4: Identify the Critical Path

The **critical path** is the longest chain of dependent models:

```
stg_orders (1.89s)
    ↓
int_orders_enriched (4.12s)
    ↓
fct_orders (8.45s)
    ↓
dim_customers (6.23s)
────────────────────────
TOTAL: 20.69s (critical path)
```

**Key insight:** Optimizing models ON the critical path has the biggest impact.

### Step 5: Analyze by Layer

Add this to the analysis:

```python
# Group by layer
staging_time = sum(t['total_seconds'] for t in timings if 'stg_' in t['node'])
intermediate_time = sum(t['total_seconds'] for t in timings if 'int_' in t['node'])
marts_time = sum(t['total_seconds'] for t in timings if t['node'] in ['fct_orders', 'dim_customers'])

print(f"\nTime by layer:")
print(f"  Staging:      {staging_time:.2f}s")
print(f"  Intermediate: {intermediate_time:.2f}s")
print(f"  Marts:        {marts_time:.2f}s")
```

---

## 📝 Part B: Implement Optimizations

### Step 6: Optimize fct_orders (Slowest Model)

Current state - check `models/marts/fct_orders.sql`:

```sql
{{
  config(
    materialized='incremental',
    unique_key='order_id',
    partition_by={
      'field': 'order_date',
      'data_type': 'date',
      'granularity': 'month'
    },
    cluster_by=['order_status', 'customer_state']
  )
}}
```

**Optimizations to consider:**

1. **Partitioning** - Already done ✅
2. **Clustering** - Already done ✅
3. **Incremental** - Already done ✅

Let's optimize `int_orders_enriched` instead:

### Step 7: Optimize int_orders_enriched

Edit `models/intermediate/int_orders_enriched.sql`:

```sql
{{
  config(
    materialized='table',  -- Consider: 'incremental' for large datasets
    partition_by={
      'field': 'order_date',
      'data_type': 'date',
      'granularity': 'month'
    },
    cluster_by=['customer_id', 'order_status']
  )
}}
```

### Step 8: Measure Improvement

```bash
# Rebuild and time the run
time dbt run --select int_orders_enriched+

# Re-run analysis
python labs/lab-06/starter/analyze_results.py
```

Compare the new timing with the original.

---

## 📝 Part C: Advanced Analysis

### Step 9: Complete the Enhanced Analysis Script

Open `labs/lab-06/starter/analyze_results.py` and implement the TODOs:

```python
def find_bottlenecks(timings: list, threshold_seconds: float = 5.0) -> list:
    """Find models that exceed the time threshold."""
    return [t for t in timings if t['total_seconds'] > threshold_seconds]

def analyze_by_status(timings: list) -> dict:
    """Group results by status."""
    from collections import defaultdict
    status_groups = defaultdict(list)
    for t in timings:
        status_groups[t['status']].append(t)
    return dict(status_groups)

def calculate_parallelization_efficiency(results: dict) -> float:
    """
    Compare actual wall time vs sum of model times.
    Higher ratio = better parallelization.
    """
    total_model_time = sum(r.get('execution_time', 0) for r in results.get('results', []))
    
    # Get wall clock time from metadata
    elapsed = results.get('elapsed_time', total_model_time)
    
    if elapsed > 0:
        # Ratio > 1 means models ran in parallel
        return total_model_time / elapsed
    return 1.0
```

### Step 10: Create a Performance Dashboard

Save analysis to a file for tracking over time:

```python
import json
from datetime import datetime

def save_performance_snapshot(timings: list, output_path: str = 'performance_history.json'):
    """Save performance metrics for historical tracking."""
    snapshot = {
        'timestamp': datetime.now().isoformat(),
        'total_time': sum(t['total_seconds'] for t in timings),
        'model_count': len(timings),
        'slowest_model': max(timings, key=lambda x: x['total_seconds']),
        'avg_time': sum(t['total_seconds'] for t in timings) / len(timings)
    }
    
    # Load existing history
    try:
        with open(output_path, 'r') as f:
            history = json.load(f)
    except FileNotFoundError:
        history = []
    
    history.append(snapshot)
    
    with open(output_path, 'w') as f:
        json.dump(history, f, indent=2)
    
    print(f"Snapshot saved to {output_path}")
```

---

## 🔍 Expected Output

### Performance Report

```
============================================================
DBT PERFORMANCE REPORT
============================================================

Model                                    Status     Time (s)  
------------------------------------------------------------
fct_orders                               success    8.45      
dim_customers                            success    6.23      
int_orders_enriched                      success    4.12      

------------------------------------------------------------
Total models: 9
Successful: 9
Errors: 0
Total time: 27.14 seconds

BOTTLENECKS (> 5.0s):
  - fct_orders: 8.45s
  - dim_customers: 6.23s

Time by layer:
  Staging:      5.00s
  Intermediate: 6.46s
  Marts:        14.68s

Parallelization efficiency: 2.3x
============================================================
```

---

## 🎯 Bonus Challenges

### Challenge 1: Trend Analysis

Track performance over multiple runs:

```python
def plot_performance_trend(history_path: str = 'performance_history.json'):
    """Plot total build time over time."""
    import matplotlib.pyplot as plt
    
    with open(history_path, 'r') as f:
        history = json.load(f)
    
    times = [h['total_time'] for h in history]
    plt.plot(times)
    plt.title('dbt Build Time Trend')
    plt.xlabel('Run')
    plt.ylabel('Total Time (s)')
    plt.savefig('performance_trend.png')
```

### Challenge 2: Automatic Recommendations

```python
def generate_recommendations(timings: list) -> list:
    """Generate optimization recommendations."""
    recommendations = []
    
    for t in timings:
        if t['total_seconds'] > 10:
            recommendations.append(
                f"⚠️ {t['node']}: Consider incremental materialization"
            )
        elif t['total_seconds'] > 5:
            recommendations.append(
                f"💡 {t['node']}: Review for optimization opportunities"
            )
    
    return recommendations
```

### Challenge 3: Slack Alerting

Send performance alerts to Slack:

```python
import requests

def send_slack_alert(webhook_url: str, message: str):
    """Send performance alert to Slack."""
    payload = {"text": message}
    requests.post(webhook_url, json=payload)

# Usage:
bottlenecks = find_bottlenecks(timings, threshold_seconds=10)
if bottlenecks:
    message = f"🐌 Slow models detected:\n" + \
              "\n".join([f"• {b['node']}: {b['total_seconds']}s" for b in bottlenecks])
    send_slack_alert(WEBHOOK_URL, message)
```

---

## ✅ Validation Checklist

Before completing this lab, verify:

- [ ] Analysis script runs without errors
- [ ] You can identify the slowest models
- [ ] You understand the critical path concept
- [ ] At least one optimization was implemented
- [ ] Performance improvement was measured

---

## 📚 Key Takeaways

1. **Artifacts are valuable** - run_results.json contains timing data
2. **Critical path matters** - Optimize models on the longest chain
3. **Parallelization helps** - Independent models run concurrently
4. **Measure before/after** - Always quantify improvements
5. **Track over time** - Performance regression detection

---

## 🔗 Resources

- [dbt Artifacts](https://docs.getdbt.com/reference/artifacts/dbt-artifacts)
- [BigQuery Optimization](https://cloud.google.com/bigquery/docs/best-practices-performance-overview)
- [Incremental Models](https://docs.getdbt.com/docs/build/incremental-models)
- [Model Configurations](https://docs.getdbt.com/reference/model-configs)

---

## 🎓 Module Complete!

Congratulations on completing all 6 labs of the Advanced dbt module!

### Summary of What You've Learned:

| Lab | Topic | Key Skills |
|-----|-------|------------|
| 01 | Custom Generic Tests | Test macros, severity config |
| 02 | Data Contracts & Unit Tests | Schema enforcement, mock data |
| 03 | DRY Macros | Jinja templating, code reuse |
| 04 | Packages & Audit Helper | Package management, model comparison |
| 05 | Slim CI | State selection, GitHub Actions |
| 06 | Performance Analysis | Artifact parsing, optimization |

### Next Steps:

1. Apply these skills to your own dbt projects
2. Explore dbt Cloud for managed CI/CD
3. Build internal packages for your organization
4. Set up monitoring dashboards
5. Get dbt certified! → [learn.getdbt.com](https://learn.getdbt.com)
