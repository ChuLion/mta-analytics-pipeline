# MTA Analytics Pipeline — Architecture

This diagram illustrates the end-to-end data lineage from raw NYC Open Data and Census APIs through the dbt Medallion layers into Tableau Public.

```mermaid
graph TD
    %% Source Nodes (API)
    subgraph Sources ["1. DATA SOURCES"]
        style Sources fill:#fcfcfc,stroke:#333,stroke-width:1px
        MTA["MTA Hourly Ridership API<br/>(wujg-7c2s)"]:::source
        CENSUS["Census Tracts API<br/>(ACS 2023)"]:::source
        PySource[Python Ingestion Scripts]:::python
    end

    %% Ingestion Links
    MTA -->|HTTPS| PySource
    CENSUS -->|HTTPS| PySource

    %% Storage Node (GCS)
    subgraph GCS ["2. RAW STORAGE (GCS)"]
        style GCS fill:#e8f0fe,stroke:#4285F4,stroke-width:1px
        Bucket[Cloud Storage Bucket<br/>Partitioned CSVs]:::gcs
    end

    %% GCS Ingestion
    PySource -->|bq load| Bucket

    %% Bronze Node (BigQuery Raw)
    subgraph Bronze ["3. BRONZE LAYER (mta_bronze)"]
        style Bronze fill:#fee,stroke:#b33,stroke-width:1px
        MTA_B2224[mta_ridership_2022_2024]:::bronze
        MTA_B2019[mta_ridership_2019]:::bronze
        MTA_B2025[mta_ridership_2025]:::bronze_inc
        CENSUS_B[census_nyc_tracts]:::bronze
    end

    %% Bronze Load
    Bucket -->|BigQuery Load| Bronze

    %% Silver Node (BigQuery Transformed)
    subgraph Silver ["4. SILVER LAYER (mta_silver)"]
        style Silver fill:#f3e5f5,stroke:#7b1fa2,stroke-width:1px
        
        subgraph Staging ["Staging Models (Views)"]
            style Staging fill:#fff,stroke:#7b1fa2,stroke-dasharray: 5 5
            StgMTA[stg_mta_ridership]:::silver
            Stg2019[stg_mta_ridership_2019]:::silver
            StgCensus[stg_census_income]:::silver
        end

        subgraph Intermediate ["Intermediate Models (Views)"]
            style Intermediate fill:#fff,stroke:#7b1fa2,stroke-dasharray: 5 5
            IntRecov[int_station_recovery]:::silver
            IntCong[int_station_congestion]:::silver
            IntUtil[int_station_utilization]:::silver
            IntCensus[int_station_census_join]:::silver
            IntCap[int_station_capacity]:::silver
        end
    end

    %% Silver Lineage
    MTA_B2224 -->|dbt| StgMTA
    MTA_B2019 -->|dbt| Stg2019
    MTA_B2025 -->|dbt| StgMTA
    CENSUS_B -->|dbt| StgCensus

    StgMTA -->|dbt| IntRecov
    Stg2019 -->|dbt| IntRecov
    StgMTA -->|dbt| IntCong
    StgMTA -->|dbt| IntUtil
    StgMTA -->|dbt| IntCensus
    StgCensus -->|dbt GIS| IntCensus
    Stg2019 -->|dbt| IntCap

    %% Gold Node (BigQuery Curated)
    subgraph Gold ["5. GOLD LAYER (mta_gold)"]
        style Gold fill:#fff3e0,stroke:#e65100,stroke-width:1px
        MartRecov[mart_recovery_scorecard]:::gold
        MartCong[mart_congestion_trigger]:::gold
        MartEff[mart_efficiency_matrix]:::gold
        MartEquity[mart_equity_view]:::gold
    end

    %% Gold Lineage
    IntRecov -->|dbt| MartRecov
    IntCong -->|dbt| MartCong
    IntCong -->|dbt| MartEff
    IntRecov -->|dbt| MartEff
    IntCensus -->|dbt| MartEquity
    IntUtil -->|dbt| MartEquity
    MartRecov -->|dbt| MartEquity

    %% BI Node (Tableau)
    subgraph Tableau ["6. BUSINESS INTELLIGENCE (Tableau Public)"]
        style Tableau fill:#e6fffa,stroke:#00897b,stroke-width:1px
        Dash1[Dashboard 1:<br/>Recovery]:::tableau
        Dash2[Dashboard 2:<br/>Congestion]:::tableau
        Dash3[Dashboard 3:<br/>Efficiency]:::tableau
        Dash4[Dashboard 4:<br/>Service Justice]:::tableau
    end

    %% Tableau Lineage
    MartRecov -->|Direct Query| Dash1
    MartCong -->|Direct Query| Dash2
    MartEff -->|Direct Query| Dash3
    MartEquity -->|Direct Query| Dash4

    %% DevOps Node
    subgraph DevOps ["7. CI/CD & ORCHESTRATION"]
        style DevOps fill:#f5f5f5,stroke:#666,stroke-width:1px
        CI[GitHub Actions<br/>dbt test on push]:::gcs
        Cron[Weekly Cron<br/>Incremental 2025]:::python
    end

    %% Node Styles
    classDef source fill:#e6fffa,stroke:#004d40,stroke-width:1px,rx:5,ry:5;
    classDef python fill:#fffde7,stroke:#fbc02d,stroke-width:1px,rx:5,ry:5;
    classDef gcs fill:#e3f2fd,stroke:#1565c0,stroke-width:1px,rx:5,ry:5;
    classDef bronze fill:#ffebee,stroke:#b71c1c,stroke-width:1px,rx:5,ry:5;
    classDef bronze_inc fill:#ffebee,stroke:#b71c1c,stroke-width:2px,stroke-dasharray: 5 5,rx:5,ry:5;
    classDef silver fill:#f3e5f5,stroke:#4a148c,stroke-width:1px,rx:5,ry:5;
    classDef gold fill:#fff3e0,stroke:#bf360c,stroke-width:1px,rx:5,ry:5;
    classDef tableau fill:#e0f2f1,stroke:#004d40,stroke-width:1px,rx:5,ry:5;