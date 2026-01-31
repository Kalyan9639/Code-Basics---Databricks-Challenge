# 🚀 RetailPulse AI  
### *Enterprise-Scale Customer Intelligence with Databricks & Spark ML*

<p align="center">
  <img src="https://img.shields.io/badge/Databricks-Platform-red?style=for-the-badge&logo=databricks" />
  <img src="https://img.shields.io/badge/Apache%20Spark-ML-orange?style=for-the-badge&logo=apachespark" />
  <img src="https://img.shields.io/badge/MLflow-Governed-blue?style=for-the-badge&logo=mlflow" />
</p>

<p align="center">
  <img src="https://img.shields.io/badge/Big%20Data-1M%2B%20Records-success?style=for-the-badge" />
  <img src="https://img.shields.io/badge/Accuracy-88.95%25-brightgreen?style=for-the-badge" />
  <img src="https://img.shields.io/badge/Architecture-Medallion-purple?style=for-the-badge" />
</p>

<p align="center">
  <img src="https://img.shields.io/badge/Status-Production%20Ready-success?style=for-the-badge" />
  <img src="https://img.shields.io/badge/Use%20Case-Retail%20Analytics-blueviolet?style=for-the-badge" />
  <img src="https://img.shields.io/badge/Challenge-Databricks%2014--Day%20AI%20Challenge-black?style=for-the-badge" />
</p>


<p align="center">
  <img src="Output Images/Business Insight.png" width="800" />
</p>

<p align="center">
  <b>1M+ Records • Medallion Architecture • MLflow Governed • Production-Ready</b>
</p>

---

## 🧠 Executive Summary
**RetailPulse AI** is an end-to-end **data engineering + machine learning system** built on **Databricks** to predict customer value tiers at scale.  
Instead of relying on static membership labels, the system uncovers **“Hidden Elite” customers** using behavioral signals — enabling **precision marketing, smarter spend allocation, and revenue growth**.

- ✅ **Global Accuracy:** **88.95%**
- ✅ **Scale:** **1,000,000+ transactions**
- ✅ **Enterprise-grade:** Medallion Architecture, MLflow, Model Registry

---

## 🏗️ System Architecture
### Databricks Medallion Architecture

```
Raw Data ──▶ Bronze ──▶ Silver ──▶ Gold ──▶ BI / ML Consumers
(Ingest) (Clean & FE) (Predictions)
```


| Layer | Purpose | Highlights |
|-----|--------|-----------|
| **Bronze** | Raw ingestion | 1M transaction records, schema preserved |
| **Silver** | Feature engineering | Null handling, encoding, behavioral features |
| **Gold** | Business-ready | Predictions + confidence scores |

---

## 🔬 Data Engineering Highlights

### 🔹 Bronze Layer
- Raw ingestion into Databricks tables
- Schema validation & lineage tracking

### 🔹 Silver Layer (Core Intelligence)
- **Age-Bin Mode Imputation** to preserve demographic distribution
- **Target Encoding** for 50+ unique states
- **Behavioral Features** (`is_weekend`, spending patterns)
- ⚠️ **Spark Metadata Fix**  
  Resolved vector assembler conflicts by stripping categorical metadata — a real-world Spark pitfall rarely documented.

### 🔹 Gold Layer
- Final analytics table with:
  - Actual Segment
  - Predicted Segment
  - **Prediction Confidence Score**

---

## 🤖 Machine Learning Pipeline

| Component | Choice | Reason |
|--------|-------|-------|
| Model | Random Forest Classifier | Robust, interpretable, scalable |
| Tuning | 3-Fold Cross-Validation | Stable generalization |
| Depth | MaxDepth = 10 | Prevent overfitting |
| Governance | MLflow | Experiment & model tracking |


---

## 📊 Model Performance

<p align="center">
  <img src="Output Images/Global Model Accuracy.png" width="600" />
</p>

- **Overall Accuracy:** **88.95%**
- **High-Confidence Predictions (> 0.85):** 90% of Elite class

<p align="center">
  <img src="Output Images/Confidence Score Distribution.png" width="600" />
</p>

---

## 📈 Feature Importance & Business Impact

<p align="center">
  <img src="Output Images/feature_importance.png" width="650" />
</p>

| Feature | Importance | Business Action |
|------|-----------|----------------|
| **State_Target_Encoded** | 🔥 High | Optimize regional ad spend |
| **Segment_vec** | ⚡ Medium | Shift to behavior-based loyalty |
| **is_weekend** | 📊 Significant | Launch premium weekend offers |

---

## 🧩 Confusion & Risk Analysis

<p align="center">
  <img src="Output Images/Confusion Analysis.png" width="600" />
</p>

- Minimal confusion between adjacent tiers
- High precision for **Elite customers**, reducing marketing risk

---

## 📊 Databricks SQL Dashboard
**Executive-ready insights:**
- Live accuracy monitoring
- Confidence-weighted predictions
- Identification of **Hidden Elite customers**

<p align="center">
  <img src="Output Images/Average Confidence by Spend Tier.png" width="650" />
</p>

---

## 📂 Project Structure

```
RetailPulse-AI/
│
├── Output Images/
│   ├── Average Confidence by Spend Tier.png
│   ├── Business Insight.png
│   ├── Confidence Score Distribution.png
│   ├── Confusion Analysis.png
│   ├── Global Model Accuracy.png
│   └── feature_importance.png
│
├── Data loading and bronze level.ipynb
├── filling null values -- silver level.ipynb
├── Feature Engineering -- silver level.ipynb
├── Feature Importance -- gold.ipynb
├── model training -- gold.ipynb
│
└── README.md
```

---



---

## 🏁 Final Note
**RetailPulse AI** is not a demo — it’s a **production-grade blueprint** for customer intelligence systems used by modern retail enterprises.

### Built as part of the **Databricks 14-Day AI Challenge**
