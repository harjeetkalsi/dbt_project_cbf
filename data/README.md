# REAL Olist E-Commerce Data - 10K Subset

## ✅ This is AUTHENTIC Data!

**Source:** [Brazilian E-Commerce Public Dataset by Olist](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce) on Kaggle

**Period:** 2017 (Full year)  
**Subset Size:** 10,000 orders

---

## 📊 Dataset Statistics

| File | Records | Description |
|------|---------|-------------|
| `orders.csv` | 10,000 | Real orders from Olist Brazilian marketplace |
| `customers.csv` | 10,000 | Actual customers across Brazil |
| `order_items.csv` | 11,253 | Line items (avg 1.1 items per order) |
| `products.csv` | 6,124 | Real product catalog |
| `product_category_name_translation.csv` | 71 | Portuguese → English translations |

---

## 🔍 How to Verify This is Real Data

### **1. Order IDs are MD5 Hashes (like real Olist)**
```
e481f51cbdc54678b7cc49136f2d6af7  ← Real Olist format
```

### **2. Brazilian Cities and States**
```
Cities: sao paulo, rio de janeiro, brasilia, lencois paulista, resende
States: SP, RJ, MG, BA, RS
```

### **3. Portuguese Product Categories**
```
moveis_decoracao → furniture_decor
beleza_saude → health_beauty
cama_mesa_banho → bed_bath_table
```

### **4. Real 2017 Timestamps**
```
2017-10-02 10:56:33
2017-11-18 19:28:06
2017-07-09 21:57:05
```

### **5. Cross-Reference with Kaggle**
You can verify these order IDs exist in the [full Kaggle dataset](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce)!

---

## 🎓 Why Real Data Matters

✅ **Portfolio Credibility** - "I built a dbt pipeline with the Olist dataset"  
✅ **Industry Recognition** - Same dataset used in 1000+ Kaggle analyses  
✅ **Authentic Patterns** - Real customer behavior, not simulated  
✅ **Verifiable** - Can cross-check with public Kaggle dataset  
✅ **Real Data Quality Issues** - Nulls, late deliveries, cancellations  

---

## 📦 What Students Get

- **10,000 REAL transactions** from a real Brazilian e-commerce platform
- **Actual business data** with real quality issues
- **Industry-standard dataset** recognized in data science community
- **Portuguese/English** bilingual data (real-world complexity)
- **Complete relationships** (orders → items → products → categories)

---

## 🚀 Next Steps

1. Load this data to BigQuery using `setup/02_load_data_to_bigquery.sh`
2. Run `dbt run` to build models
3. Run `dbt test` to validate data quality
4. Run `dbt docs generate` to see lineage

---

## 📝 Data License

**CC BY-NC-SA 4.0** - Creative Commons Attribution-NonCommercial-ShareAlike 4.0

Original data published by Olist on Kaggle. Data has been anonymized and company names in reviews replaced with Game of Thrones house names.

---

## 🎯 This is Production-Quality Data

**This is NOT:**
- ❌ Generated/synthetic data
- ❌ Sample/demo data
- ❌ Simplified/cleaned data

**This IS:**
- ✅ Real commercial transactions
- ✅ Actual marketplace data
- ✅ Industry-standard dataset
- ✅ Used by data professionals worldwide

**Students learn with the same data pros use!** 🚀
