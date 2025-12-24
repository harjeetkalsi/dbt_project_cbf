# Advanced dbt - Real Olist E-Commerce Dataset

[![dbt](https://img.shields.io/badge/dbt-1.7+-orange.svg)](https://www.getdbt.com/)
[![BigQuery](https://img.shields.io/badge/BigQuery-enabled-blue.svg)](https://cloud.google.com/bigquery)
[![License](https://img.shields.io/badge/License-CC%20BY--NC--SA%204.0-lightgrey.svg)](https://creativecommons.org/licenses/by-nc-sa/4.0/)

Production-grade dbt project for the **Advanced dbt** training module. Built with **10,000 real orders** from Olist Brazilian E-commerce.

---

## 🎯 Learning Objectives

After completing this module, you will be able to:

- ✅ Build robust tests with dbt_expectations and custom generic tests
- ✅ Implement data contracts (dbt 1.5+) and unit tests (dbt 1.8+)
- ✅ Create reusable macros and leverage dbt packages
- ✅ Set up CI/CD pipelines with GitHub Actions
- ✅ Implement Slim CI for faster PR validation
- ✅ Monitor pipeline performance and troubleshoot issues
- ✅ Use dbt artifacts for custom tooling

---

## 🚀 Quick Start

### Prerequisites

- Python 3.8+
- Google Cloud Platform account
- dbt 1.7+ (`pip install dbt-bigquery`)
- Basic dbt knowledge (models, ref, source)

### Setup (15 minutes)

```bash
# 1. Clone the repository
git clone <repository-url>
cd dbt_advanced_olist

# 2. Run setup script (creates venv, installs dbt)
chmod +x scripts/setup.sh
./scripts/setup.sh

# 3. Configure profiles.yml
cp profiles.yml.example ~/.dbt/profiles.yml
# Edit ~/.dbt/profiles.yml with your GCP project ID and keyfile path

# 4. Load data to BigQuery (REQUIRED!)
export GCP_PROJECT_ID="your-project-id"
chmod +x scripts/load_data_to_bigquery.sh
./scripts/load_data_to_bigquery.sh

# 5. Test connection
source venv/bin/activate
dbt debug

# 6. Build the project
dbt build
```

### Manual Data Loading (Alternative)

If the script doesn't work, load CSVs manually via BigQuery Console:
1. Go to BigQuery Console → Create Dataset named `raw_data`
2. For each CSV in `data/` folder: Create Table → Upload → Select CSV
3. Use schema auto-detection or refer to `scripts/load_data_to_bigquery.sh` for column types

---

## 📁 Project Structure

```
dbt_advanced_olist/
├── models/
│   ├── staging/           # Source cleaning (views)
│   │   ├── _sources.yml   # Source definitions + freshness
│   │   ├── _staging.yml   # Schema tests
│   │   └── stg_*.sql      # Staging models
│   ├── intermediate/      # Business logic joins
│   │   └── int_*.sql      
│   └── marts/             # BI-ready tables
│       ├── _marts.yml     # Data contracts + unit tests
│       ├── dim_customers.sql
│       └── fct_orders.sql
├── tests/
│   ├── generic/           # Reusable test macros
│   │   ├── test_is_positive.sql
│   │   ├── test_is_not_negative.sql
│   │   ├── test_is_recent.sql
│   │   └── test_row_count_delta.sql
│   └── singular/          # Custom SQL tests
│       ├── assert_orders_have_items.sql
│       └── assert_delivery_dates_logical.sql
├── macros/
│   ├── limit_in_dev.sql
│   ├── generate_date_parts.sql
│   ├── safe_divide.sql
│   └── logging_hooks.sql
├── labs/                  # 🆕 Hands-on exercises (6 labs)
│   ├── lab-01/            # Custom Generic Tests
│   ├── lab-02/            # Data Contracts & Unit Tests
│   ├── lab-03/            # Creating DRY Macros
│   ├── lab-04/            # Packages & Audit Helper
│   ├── lab-05/            # Setting Up Slim CI
│   └── lab-06/            # Analyzing Pipeline Performance
├── seeds/                 # Reference data
├── snapshots/             # SCD Type 2
├── analyses/              # Ad-hoc queries
└── .github/workflows/     # CI/CD pipelines
    ├── ci.yml             # PR validation
    └── deploy.yml         # Production deployment
```

---

## 🧪 Labs Overview

This module includes **6 hands-on labs** (2 per session) with complete step-by-step instructions.

### Session 1: Building Robust Tests

| Lab | Topic | Duration | Description |
|-----|-------|----------|-------------|
| [Lab 01](labs/lab-01/README.md) | Custom Generic Tests | 25 min | Create reusable `is_recent` test macro |
| [Lab 02](labs/lab-02/README.md) | Data Contracts & Unit Tests | 30 min | Enforce schema + test business logic |

### Session 2: Macros & Packages

| Lab | Topic | Duration | Description |
|-----|-------|----------|-------------|
| [Lab 03](labs/lab-03/README.md) | Creating DRY Macros | 30 min | Build `generate_date_columns` macro |
| [Lab 04](labs/lab-04/README.md) | Packages & Audit Helper | 30 min | Use dbt_utils and compare model versions |

### Session 3: CI/CD & Production

| Lab | Topic | Duration | Description |
|-----|-------|----------|-------------|
| [Lab 05](labs/lab-05/README.md) | Setting Up Slim CI | 30 min | Configure GitHub Actions + state selection |
| [Lab 06](labs/lab-06/README.md) | Analyzing Pipeline Performance | 25 min | Parse artifacts + identify bottlenecks |

### Lab Structure

Each lab folder contains:
```
lab-XX/
├── README.md        # 📖 Full instructions, background, step-by-step guide
├── starter/         # 💡 Template files to complete
│   └── *.sql/py/yml
└── solution/        # ✅ Complete working solutions
    └── *.sql/py/yml
```

---

## 🧪 Testing Features

### Schema Tests (dbt_expectations)
```yaml
columns:
  - name: order_total
    tests:
      - dbt_expectations.expect_column_values_to_be_between:
          min_value: 0
          max_value: 50000
```

### Custom Generic Tests
```sql
{% test is_positive(model, column_name) %}
select * from {{ model }}
where {{ column_name }} <= 0
{% endtest %}
```

### Data Contracts (dbt 1.5+)
```yaml
models:
  - name: dim_customers
    config:
      contract:
        enforced: true
    columns:
      - name: customer_id
        data_type: string
        constraints:
          - type: not_null
          - type: primary_key
```

### Unit Tests (dbt 1.8+)
```yaml
unit_tests:
  - name: test_vip_segment
    model: dim_customers
    given:
      - input: ref('stg_orders')
        rows:
          - {customer_id: 'c1', order_total: 600}
    expect:
      rows:
        - {customer_id: 'c1', customer_segment: 'VIP'}
```

---

## 🔧 Key Commands

```bash
# Build everything
dbt build

# Run only modified + downstream (Slim CI)
dbt build --select state:modified+ --state prod-manifest/

# Test specific model
dbt test --select dim_customers

# Run unit tests only
dbt test --select test_type:unit

# Run generic tests only
dbt test --select test_type:generic

# Generate documentation
dbt docs generate && dbt docs serve

# Check source freshness
dbt source freshness

# Compile to see generated SQL
dbt compile --select model_name
```

---

## 📦 Packages Used

| Package | Version | Purpose |
|---------|---------|---------|
| dbt_utils | 1.1.1 | Utilities (surrogate_key, date_spine) |
| dbt_expectations | 0.10.3 | Great Expectations-style tests |
| dbt_audit_helper | 0.9.0 | Compare model outputs |
| codegen | 0.12.1 | Generate schema YAML |

Install packages:
```bash
dbt deps
```

---

## 🌐 CI/CD Pipeline

```
┌─────────────┐     ┌─────────────┐     ┌─────────────┐
│   Branch    │────▶│  Pull Request│────▶│    Main     │
│  (develop)  │     │   (CI test)  │     │  (deploy)   │
└─────────────┘     └─────────────┘     └─────────────┘
      │                    │                    │
      ▼                    ▼                    ▼
   dbt build          Slim CI:              Full build
   (local)         state:modified+         + artifacts
```

---

## 📊 Lab Completion Tracking

Use this checklist to track your progress:

- [ ] **Lab 01**: Custom Generic Tests - `is_recent` test created and running
- [ ] **Lab 02**: Data Contracts - Contract enforced on `dim_customers`, unit tests passing
- [ ] **Lab 03**: DRY Macros - `generate_date_columns` macro compiles correctly
- [ ] **Lab 04**: Audit Helper - Model comparison executed successfully
- [ ] **Lab 05**: Slim CI - GitHub Actions workflow configured and tested
- [ ] **Lab 06**: Performance Analysis - Bottlenecks identified and recommendations generated

---

## 🎓 Learning Outcomes

By completing all labs, you will demonstrate the ability to:

| Outcome | Verified In |
|---------|-------------|
| Build a pipeline of models | All labs |
| Apply testing to the models | Labs 01, 02 |
| Pick out problems when alerts fire | Lab 06 |
| Review a pipeline and locate bottlenecks | Lab 06 |
| Create reusable, maintainable code | Labs 03, 04 |
| Set up production-grade CI/CD | Lab 05 |

---

## 📚 Additional Resources

- [dbt Documentation](https://docs.getdbt.com/)
- [dbt_expectations](https://github.com/calogica/dbt-expectations)
- [dbt_audit_helper](https://github.com/dbt-labs/dbt-audit-helper)
- [Olist Dataset (Kaggle)](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce)
- [dbt Best Practices](https://docs.getdbt.com/guides/best-practices)
- [dbt Certification](https://learn.getdbt.com/)

---

## 📝 License

Dataset: CC BY-NC-SA 4.0 (Olist via Kaggle)

---

**Built for Coding Black Females - Advanced dbt Module** 🚀
