# banking-fraud-analytics
End-to-End Banking Fraud Analytics Platform built on Microsoft Fabric and Delta Lake. Implements a Medallion Architecture (Bronze, Silver, Gold) with PySpark data quality gates, automated orchestration pipelines, and temporal risk scoring.

The Banking Fraud Analytics Platform is an enterprise-grade data engineering solution designed to ingest, validate, enrich, and model large-scale transactional data for fraud detection and auditing. Built using Microsoft Fabric, PySpark, and Delta Lake, the platform implements a robust Medallion Architecture to ensure data governance, quality control, and seamless dimensional analytics.

Key Features
Medallion Architecture: Multi-tier processing pipeline segregating raw data (Bronze), cleansed/audited records (Silver), and star-schema analytical models (Gold).

Automated Quality Gates: Pre-ingestion validation enforcing null-threshold boundaries and schema enforcement on high-volume transaction datasets.

Metadata & PII Governance: Automatic tracking of ingestion timestamps, run IDs, and column-level governance comments for sensitive financial attributes (nameOrig, nameDest).

Temporal Risk Scoring: Dynamic dimension modeling generating time-based risk buckets (e.g., late-night high-risk transaction windows) and MD5 surrogate keys for fact-dimension joining.

Orchestrator Lifecycle Management: Decoupled file archiving driven by pipeline execution status to ensure atomic, failure-resilient reprocessing.

Performance Optimization: Table compaction (OPTIMIZE) and history maintenance (VACUUM) integrated directly into Lakehouse pipeline runs.

Tech Stack
Platform: Microsoft Fabric / Lakehouse

Engine & Languages: PySpark, T-SQL, Delta Lake

Orchestration: Fabric Data Factory Pipelines

Storage & Catalog: Delta Tables (Parquet) with Partitioning
