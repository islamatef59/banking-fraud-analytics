# Enterprise Banking Fraud Analytics & Lakehouse Platform
End-to-End Microsoft Fabric, PySpark & Delta Lake Solution

An enterprise-grade Data Engineering & Advanced Analytics Lakehouse architecture built on Microsoft Fabric using PySpark and Delta Lake. The platform ingests, sanitizes, enriches, and models financial transaction datasets to deliver real-time risk scoring, statistical anomaly detection, SCD Type 2 dimension tracking, and Power BI-ready Star Schemas[cite: 18].

## 📐 Architecture Overview

The system follows a production-grade Medallion Architecture (Bronze → Silver → Gold) with integrated security, automated data quality gates, quarantine mechanics, and pipeline orchestration alerts[cite: 18].
<img width="300" height="300"  alt="Screenshot 2026-10-13 013215" src="https://github.com/user-attachments/assets/974bc2d2-1b99-467d-a120-cba1e408b94b" />



## ✨ Key Technical Features

* **Cryptographic PII Governance:** Salted SHA-256 anonymization for customer/account identifiers ensuring compliance with financial data privacy standards (GDPR/PCI-DSS)[cite: 18].
* **Advanced PySpark Analytics & Feature Engineering:**[cite: 18]
  * **Rolling Velocity Windows:** Tracks transaction frequency and volumetric cumulative sums over dynamically tuned steps[cite: 18].
  * **Statistical Anomaly Detection:** Calculates rolling population averages and standard deviations to compute real-time Z-Scores for outlier flags[cite: 18].
  * **Data Integrity Audit:** Validates balance math (`oldbalanceOrg - amount == newbalanceOrig`) with automated quarantine routing for corrupted payloads[cite: 18].
* **Dimensional Modeling (Star Schema):**[cite: 18]
  * **DimAccount (SCD Type 2):** Historical tracking of account state changes (`is_current`, `valid_from`, `valid_to`)[cite: 18].
  * **FactTransaction:** High-performance fact table built via idempotence-guaranteed Delta MERGE statements[cite: 18].
* **Lakehouse Optimization & Maintenance:** Delta Table `OPTIMIZE` execution with `ZORDER BY` clustering, automated `VACUUM` retention routines, and Change Data Feed enablement[cite: 18].
* **Production Pipeline Monitoring:** Automated Teams/Webhook alert notification scripts for pipeline success and failure telemetry tracking[cite: 18].

## 🗄️ Database & Schema Design

### Star Schema Entity-Relationship Matrix
<img width="400" height="300" alt="Screenshot 2026-10-13 013215" src="https://github.com/user-attachments/assets/974bc2d2-1b99-467d-a120-cba1e408b94b" />


## 🚀 Getting Started & Execution Flow

### Prerequisites
* Microsoft Fabric Workspace (or Apache Spark 3.4+ environment)[cite: 18].
* Delta Lake 2.4+[cite: 18].
* Python 3.10+ with `pyspark`, `delta-spark`, and `requests`[cite: 18].

### Execution Order
1. **Ingestion Layer (`01_Bronze_Ingestion.ipynb`)**[cite: 18]
   * Configures Delta table properties (CDF, Optimize Write)[cite: 18].
   * Reads raw financial CSV payloads into PySpark and attaches ingestion metadata[cite: 18].
   * Writes initial raw Delta tables into `bronze.raw_transactions`[cite: 18].
2. **Silver Engineering (`02_Silver_Transformation.ipynb`)**[cite: 18]
   * Appends dynamic salt keys and hashes PII fields (`nameOrig_hashed`, `nameDest_hashed`)[cite: 18].
   * Evaluates PySpark Window functions for rolling averages, standard deviations, and balance delta flags[cite: 18].
   * Persists cleansed data into `silver.cleansed_transactions`[cite: 18].
3. **Gold Modeling & Quality Control (`03_Gold_Dimensional_Modeling.ipynb`)**[cite: 18]
   * Evaluates mathematical sanity rules and routes failed rows into `gold.quarantine_transactions`[cite: 18].
   * Executes SCD Type 2 dimension updates using Delta MERGE logic on `dim_account`[cite: 18].
   * Upserts production facts into `gold.fact_transaction` and optimizes via `ZORDER BY (step)`[cite: 18].
4. **Orchestration Telemetry (`00_Pipeline_Alerts.ipynb`)**[cite: 18]
   * Integrates pipeline execution hooks with webhook notification payloads for automated operational alerts[cite: 18].

## 🧪 Data Quality & Security Rules

| Metric / Rule | Implementation Logic | Enforcement Action |
| :--- | :--- | :--- |
| **PII Hashing** | `sha2(concat(col("nameOrig"), lit(SALT)), 256)` | Irreversible masking applied at Silver layer[cite: 18] |
| **Balance Integrity** | `abs((oldbalanceOrg - amount) - newbalanceOrig) <= 0.01` | Mathematical sanity flag (`is_mathematically_sane`)[cite: 18] |
| **Anomaly Scoring** | `(Amount - μ_step) / σ_step > 3.0` | Tagged as high statistical outlier risk[cite: 18] |
| **Data Quarantine** | Insane/corrupted records routed to Delta Quarantine | Excluded from analytical Gold Fact tables[cite: 18] |

## 📊 Analytics & Reporting Capabilities

By applying **DirectLake Mode** in Power BI directly over the Gold Layer Star Schema, this platform enables high-throughput dashboards for:[cite: 18]
* **Real-time Velocity Outlier Detection:** Interactive filtering on high Z-Score transactions[cite: 18].
* **Account Risk Profiling:** Historical analysis of flag frequencies across customer vs. merchant entities[cite: 18].
* **Loss Exposure Metrics:** Aggregated fraud occurrence tracking segmented by transaction type and execution steps[cite: 18].
