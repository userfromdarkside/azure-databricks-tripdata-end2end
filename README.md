# azure-databricks-tripdata-end2end
An end-to-end data pipeline using Azure Databricks and Delta Lake, applying the Medallion Architecture to transform raw trip data into actionable business KPIs

#  Azure Databricks Trip Analytics Pipeline Project

##  Project Overview
This project implements a real-world **big data pipeline** using the **Medallion Architecture** (Bronze → Silver → Gold) on Azure Databricks and Delta Lake. It ingests, cleans, transforms, models, and aggregates trip and ride rating datasets.

**Technologies Used:**
- Azure Data Lake Storage Gen2 (ADLS Gen2)
- Azure Databricks (Delta Lake, Databricks Jobs)
- Azure Data Factory (ADF) + Logic Apps (for scheduling and alerting)
- Python (PySpark) + SQL
- Delta Lake format for ACID transaction tables

##  Architecture
- **Bronze Layer:** Ingested raw data from Gen2 
- **Silver Layer:** Cleaned and transformed dimension and fact tables (dim_rider, dim_driver, dim_location, dim_date, fact_trip)
- **Gold Layer:** Monthly aggregated KPIs (total trips, revenue, avg rating) per city
- **Orchestration:** A master notebook that runs all notebooks in sequence, handles skipping/retry, and supports scheduling

##  Project Structure
```
/notebooks
    1_ingest_raw_data_to_bronze 
    3_create_dim_rider
    4_create_dim_driver
    5_create_dim_location
    6_create_dim_date
    7_create_fact_trip
    8_create_gold_trip_summary (SQL version)
    9_orchestration_pipeline
```

##  Features
- **Data Modeling:** Star Schema Design
- **Surrogate Keys:** RiderID, DriverID, LocationID, DateID
- **PySpark Transformations:** Data cleaning, enrichment
- **SQL Transformations:** Gold layer KPI aggregation
- **Orchestration:** Databricks Job / ADF pipeline
- **Monitoring:** (Optional) Email notifications on pipeline failure

##  Business KPIs Produced (Gold Layer)
- Total Completed Trips
- Total Revenue
- Average Customer Rating
- Average Driver Rating
- Average Trip Delay (mins)
- Grouped by **City + Month**

##  How to Run
1. Upload all notebooks to Azure Databricks Workspace.
2. Upload raw datasets to Azure Data Lake Gen2.
3. Run the Orchestration notebook (`9_orchestration_pipeline`).
4. Schedule using Databricks Job or Azure Data Factory.
5. Query or visualize Gold layer (`gold.monthly_city_kpis`) in Power BI / Databricks SQL.

---

