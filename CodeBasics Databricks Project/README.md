# 📊 RetailPulse AI: 1M Row Customer Intelligence System
### *Predictive Value Segmentation using Medallion Architecture & Spark ML*

---

## 🚀 Project Overview
**RetailPulse AI** is an end-to-end data engineering and machine learning pipeline designed to predict customer spending tiers across 1 million transaction records. By moving beyond static membership labels, this system identifies "Hidden Elite" customers with **88.95% accuracy**, allowing for high-precision marketing and resource allocation.



---

## 🛠️ Technical Architecture & Workflow
This project strictly adheres to the **Databricks Medallion Architecture**, ensuring data quality and lineage at every stage.

### 1. Data Engineering (Medallion Layers)
* **Bronze Layer:** Raw ingestion of 1,000,000 transaction records into Unity Catalog.
* **Silver Layer (The Engine):** * **Advanced Imputation:** Handled missing demographic data using **Age-Bin Mode Imputation** to preserve data distribution.
    * **Feature Engineering:** Engineered `is_weekend` from temporal data and implemented **Target Encoding** for 50+ unique States.
    * **Technical Beauty Fix:** Resolved a critical Spark metadata conflict by programmatically stripping categorical attributes from encoded vectors, ensuring model compatibility.
* **Gold Layer:** Final "Business-Ready" table containing Actuals, Predictions, and **AI Confidence Scores** for direct BI consumption.

### 2. Machine Learning Pipeline
* **Model:** Random Forest Classifier (Chosen for its robustness and interpretability).
* **Optimization:** Hyperparameter tuning via **3-Fold Cross-Validation** (MaxDepth: 10, NumTrees: 100).
* **Governance:** Fully integrated with **MLflow** for experiment tracking and registered in the **Databricks Model Registry** for version control.

---

## 📈 Key Business Insights
Our AI doesn't just predict; it provides a roadmap for growth. 

| Feature | Importance | Business Strategy |
| :--- | :--- | :--- |
| **State_Target_Encoded** | **High** | Optimize logistics and regional ad-spend in high-impact states. |
| **Segment_vec** | **Medium** | Transition from "Membership-based" to "Behavior-based" loyalty tiers. |
| **is_weekend** | **Significant** | Launch 40% more 'Elite' promotions on Friday evenings to capture peak spending. |

---

## 🖥️ Databricks SQL Dashboard
The final output is a live executive dashboard that bridges the gap between AI and Decision Making.

* **Precision Audit:** Real-time monitoring of our **88.95% accuracy** rate.
* **Confidence Profile:** Highlighting that 90%+ of Tier-1 (Premium) predictions have a confidence score > 0.85.
* **Opportunity Map:** Identifying "Hidden Champions"—customers predicted as Elite despite having lower current membership levels.



---

## 🏆 Evaluation Highlights (Top-Rank Criteria)
* **Scalability:** Processed **1,000,000 records** using Spark's distributed computing.
* **AI Innovation:** Integrated **Confidence Scores** into the Gold layer to allow for risk-adjusted business decisions.
* **Technical Rigor:** Documented resolution of Spark UDT metadata conflicts during the vector assembly process.
* **Governance:** Every model iteration is audited and logged in **MLflow**, fulfilling strict enterprise standards.

---

## 📂 Project Structure
```text
├── notebooks/
│   ├── 01_Data_Ingestion_Bronze
│   ├── 02_Feature_Eng_Silver
│   └── 03_Model_Training_Gold
├── artifacts/
│   ├── feature_importance.png
│   └── ml_model_v1
└── README.md
```

---

## 🏁 Conclusion
RetailPulse AI demonstrates that with the right architecture, big data can be transformed into high-precision strategy. 
By combining Technical Beauty with Business Value, this project provides a production-ready template for retail intelligence.

---

> Created as part of the Databricks 14-Day AI Challenge.
