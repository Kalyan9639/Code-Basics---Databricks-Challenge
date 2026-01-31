# 🚀 RetailPulse AI  
### *Enterprise-Scale Customer Intelligence with Databricks & Spark ML*

<p align="center">
  <img src="Output Images/Business Insight.png" width="800" />
</p>

<p align="center">
  <img src="https://img.shields.io/badge/Databricks-Platform-red?style=for-the-badge&logo=databricks" />
  <img src="https://img.shields.io/badge/Apache%20Spark-ML-orange?style=for-the-badge&logo=apachespark" />
  <img src="https://img.shields.io/badge/MLflow-Governed-blue?style=for-the-badge&logo=mlflow" />
</p>

<p align="center">
  <img src="https://img.shields.io/badge/Architecture-Medallion-purple?style=for-the-badge" />
</p>

<p align="center">
  <img src="https://img.shields.io/badge/Use%20Case-Retail%20Analytics-blueviolet?style=for-the-badge" />
  <img src="https://img.shields.io/badge/Challenge-Databricks%2014--Day%20AI%20Challenge-green?style=for-the-badge" />
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
(Ingest) (Clean & Feature Engineering) (Predictions)
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

## 🤖 Machine Learning Approach

Two Random Forest variants were evaluated:

- **Baseline Random Forest Classifier**
- **Hyperparameter-Tuned Random Forest**

Both models achieved **statistically identical performance** across accuracy and class-wise metrics.

### 📌 Final Model Selection Rationale
The **baseline Random Forest** was selected for production because:

- Comparable accuracy to the tuned model
- Lower training and inference complexity
- Faster retraining in distributed Spark environments
- Reduced operational and governance overhead

> This decision reflects a **production-first ML mindset**, prioritizing simplicity and reliability over marginal gains.


---

## 📊 Model Performance

<p align="center">
  <img src="Output Images/Global Model Accuracy.png" width="600" />
</p>

- **Overall Accuracy:** **88.95%**


<p align="center">
  <img src="Output Images/Confidence Score Distribution.png" width="650" />
</p>

- The model produces a **well-spread confidence distribution**, indicating it is not defaulting to majority-class predictions.
- Most predictions fall in the **moderate-to-high confidence range (≈ 0.45 – 0.75)**, showing stable decision boundaries.
- Confidence scores vary meaningfully across customers, enabling **risk-aware downstream decision making** rather than binary classification.
- This confidence-aware output allows business teams to **prioritize actions only on high-certainty predictions**, reducing operational risk.


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

- Strong diagonal concentration confirms **high agreement between actual and predicted segments**.
- Most misclassifications occur **between adjacent tiers** (Basic ↔ Elite, Elite ↔ Premium), which is expected in behavioral segmentation problems.
- Off-diagonal Elite–Premium overlap highlights **“Hidden Elites”** — customers behaving like higher-tier spenders despite lower current labels.
- There is minimal extreme misclassification (Basic ↔ Premium), indicating **low business risk** from incorrect targeting.


---

## 📊 Databricks SQL Dashboard

<p align="center">
  <img src="Output Images/Average Confidence by Spend Tier.png" width="650" />
</p>

- The predicted distribution shows a **larger-than-expected Premium customer volume**, revealing untapped high-value segments.
- Average confidence is **highest for Premium and Basic tiers**, indicating strong model reliability at the revenue extremes.
- Slightly lower confidence for Elite predictions reflects natural overlap between mid-tier and high-tier customer behavior.
- These insights support **tier-specific strategies**:
  - High-confidence Premium → immediate upsell & retention
  - Moderate-confidence Elite → targeted experimentation

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
