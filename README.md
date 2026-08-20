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
The ELT pipeline is orchestrated via Databricks Notebooks, strictly adhering to the Medallion Architecture data quality standards:

### Bronze Layer (Raw Ingestion)
- `01_Ingestion_API_IBGE` — Automated extraction of raw demographic data directly from IBGE APIs.
- `03_Ingestion_Automated_SINESP` — Ingestion of historical public safety and crime records (SINESP).

### Silver Layer (Cleansing & Conformed Data)
- `02_Silver_Cleansing_IBGE` — Sanitization of IBGE demographics, standardizing municipality strings and state codes.
- `04_Silver_Cleansing_SINESP` — Cleansing of crime data, handling nulls, and applying composite key business rules.
- `05_Ingestion_Silver_Economic_PIB` — Processing and standardization of regional GDP economic data.
- `06_Ingestion_Silver_Fleet_Detailed` — Processing of vehicle fleet datasets (SENATRAN).
- `07_Ingestion_Silver_IDH` — Normalization of Human Development Index (HDI) metrics.

### Gold Layer (Star Schema & Intelligence)
- `08_Gold_Public_Safety_Macro` — The core Fact Table integrating crime occurrences with demographics (rates per 100k inhabitants).
- `09_Gold_Advanced_Analytics_Engine` — Statistical engine calculating Z-Scores, anomaly detection, and the Social & Criminal Vulnerability Index (IVSC).


## Tech Stack
- **Big Data & Processing:** Apache Spark, Databricks, Delta Lake.
- **Data Modeling:** Star Schema, Medallion Architecture.
- **Language:** PySpark, SQL.
- **Visualization:** Power BI.


## Disclaimer
Datasets integrated from public sources; some dimensions (such as IDH and Fleet distributions) were enriched or simulated for architectural and pipeline demonstration purposes. This project focuses on demonstrating senior-level Data Engineering practices, Medallion Architecture implementation, and Star Schema design principles.


## Architecture Diagram

```mermaid
graph TD
    %% Sources
    subgraph Sources ["Public Data Sources"]
        SINESP["SINESP (Crime Records)"]
        IBGE["IBGE (Demographics & GDP)"]
        SENATRAN["SENATRAN (Vehicle Fleet)"]
        IDH["UNDP/Ipea (IDH Metrics)"]
    end

    %% Bronze Layer
    subgraph Bronze ["Bronze Layer (Raw Ingestion)"]
        B1["Raw Delta Tables (Zero Transformation)"]
    end

    %% Silver Layer
    subgraph Silver ["Silver Layer (Cleansing & Standardization)"]
        S1["String Normalization & Sanitization"]
        S2["Composite Keys (Municipality + State)"]
        S3["Deduplication & Null Handling"]
    end

    %% Gold Layer
    subgraph Gold ["Gold Layer (Business Intelligence & Star Schema)"]
        G1["gold_public_safety_macro (Fact Table)"]
        G2["gold_advanced_analytics_intelligence (Z-Score, IVSC, Tiers)"]
    end

    %% Consumption
    subgraph Consumption ["Consumption Layer"]
        BI["Power BI Command Center"]
    end

    %% Flow Connections
    SINESP --> B1
    IBGE --> B1
    SENATRAN --> B1
    IDH --> B1

    B1 --> S1 --> S2 --> S3

    S3 --> G1
    S1 --> G1
    G1 --> G2

    G1 --> BI
    G2 --> BI
