# Public-Safety-Intelligence-Lakehouse

## Overview
A professional-grade Data Lakehouse solution built on **Databricks**, designed to integrate heterogeneous public datasets. By applying the **Medallion Architecture (Bronze/Silver/Gold)** and **Star Schema dimensional modeling**, this project transforms raw public safety, demographic, economic, and infrastructure data into actionable intelligence for urban management and policy-making.


## Architectural Framework & Data Flow
The pipeline follows a modern Lakehouse architecture to ensure data lineage, ACID compliance (via Delta Lake), and high-performance analytical queries:

1. **Bronze Layer (Raw Ingestion):** Ingestion of raw data from public sources (SINESP crime records, IBGE demographics & GDP, SENATRAN vehicle fleet, and UNDP/Ipea IDH metrics) with zero transformation, preserving raw formats.
2. **Silver Layer (Cleansing & Standardization):** 
   - Strict string sanitization and text normalization for municipal names (`municipality_name_clean`).
   - Implementation of robust composite keys (`municipality_name_clean` + `state_code`) to resolve critical data integrity issues involving homonymous municipalities across different Brazilian states.
   - Elimination of data duplication and handling of missing or null administrative records.
3. **Gold Layer (Business Datarts & Analytics):**
   - **`gold_public_safety_macro`**: Aggregated analytical facts providing normalized metrics like Crime Rate per 100k inhabitants and Theft Rate per 1k fleet units.
   - **`gold_advanced_analytics_intelligence`**: Statistical machine learning and analytics engine featuring Z-Score outlier detection, Social & Criminal Vulnerability Index (IVSC), and temporal rolling averages.


## Key Technical Highlights
- **Granularity Control & Integrity Checks:** Solved complex homonymous municipality collisions using strict state-level composite grouping, eliminating cartesian product inflation during multi-table joins.
- **Advanced Data Science Engine:**
  - **Z-Score Analytics:** Statistical anomaly detection to identify crime hotspots relative to state averages.
  - **IVSC (Social & Criminal Vulnerability Index):** Custom composite metric blending crime intensity with inverted Human Development Index (IDH).
  - **Economic Tiers:** Automated segmentation of municipalities based on GDP per capita (`High Economy`, `Medium Economy`, `Emerging Economy`).
- **Delta Lake Optimization:** Use of Delta format, schema enforcement, and `overwrite` strategies with clean metadata handling to prevent metadata mismatches.


## Business Intelligence & Power BI Integration
Designed to seamlessly integrate with **Power BI**, providing a command-center layout that enables stakeholders to:
- Correlate municipal economic prosperity (GDP/IDH) with public safety incidents.
- Track vehicle fleet security risks mapped against local crime rates.
- Isolate statistical outliers using pre-calculated Z-Scores for prioritized resource allocation.


## Project Structure
- `01_Ingestion_Bronze_Sinesp` — Raw crime data ingestion.
- `02_Silver_Cleansing_Sinesp` — Crime data sanitization and composite key structuring.
- `03_Ingestion_Silver_IBGE_Population` — Population demographics pipeline.
- `04_Ingestion_Silver_Economic_PIB` — GDP and economic indicators pipeline.
- `05_Ingestion_Silver_Fleet_Detailed` — Vehicle fleet asset integration.
- `06_Ingestion_Silver_IDH` — Human Development Index processing.
- `07_Gold_Public_Safety_Macro` — Star Schema macro fact table materialization.
- `08_Gold_Advanced_Analytics_Engine` — Advanced statistical intelligence & ML features.


## Tech Stack
- **Big Data & Processing:** Apache Spark, Databricks, Delta Lake.
- **Data Modeling:** Star Schema, Medallion Architecture.
- **Language:** PySpark, SQL.
- **Visualization:** Power BI.


## Disclaimer
Datasets integrated from public sources; some dimensions (such as IDH and Fleet distributions) were enriched or simulated for architectural and pipeline demonstration purposes. This project focuses on demonstrating senior-level Data Engineering practices, Medallion Architecture implementation, and Star Schema design principles.
