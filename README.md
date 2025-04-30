# Toll Pass Streaming Data Pipeline 🚗📀

An end-to-end enterprise-grade data engineering project simulating real-time toll pass event streaming with fraud detection, GDPR masking, and a Kimball-style data warehouse on Snowflake.

## 📆 Architecture Overview
This project mimics a **real-time streaming pipeline** from ingestion to BI-ready data warehouse using Azure, Databricks, Delta Lake, Snowflake, and ADF.

![ERD and Architecture Diagram](link-to-uploaded-image)

## ⚖️ Tech Stack
- **Azure**: Event Hub, ADLS Gen2, Key Vault, Function Apps, ADF
- **Data Processing**: Python, PySpark, Delta Lake
- **Orchestration**: Azure Data Factory, Snowflake Tasks
- **Storage Format**: AVRO ➔ JSON ➔ DELTA (partitioned by event_date)
- **Warehouse**: Snowflake (Bronze ➔ Silver ➔ Gold)
- **BI Tools**: Power BI (notebook + SQL access)
- **Security**: Key Vault secrets, GDPR masking policies (EU-compliant)

---

## ✨ Key Features
- ✅ **Simulated Streaming**: Azure Function App generates realistic toll events every minute
- ✅ **Secure Streaming**: Events pushed to Azure Event Hub with Managed Identity
- ✅ **Schema-on-Read**: Avro converted to JSON & structured with a defined schema
- ✅ **Delta Lake Storage**: Partitioned and optimized Delta format in ADLS
- ✅ **Orchestrated ETL**: ADF triggers Databricks notebook ➔ transfers results to Blob ➔ Snowflake COPY
- ✅ **Data Masking**: GDPR masking policy applied to `license_plate` column
- ✅ **Kimball DW Design**: Includes SCD Type 4, Junk Dim, Mini-Dim, Role-Playing Time Dim, and Fact Table

---

## 💡 Data Flow

1. **Toll Event Generation**  
   Azure Function App ➔ Generates mock JSON events ➔ Sends to Event Hub

2. **Event Ingestion & Capture**  
   Azure Event Hub ➔ Avro Files ➔ Stored in ADLS Gen2 "capture" container

3. **Data Transformation**  
   Databricks ➔ Reads Avro ➔ Parses JSON ➔ Casts schema ➔ Adds `event_date` ➔ Writes to Delta format in "delta" container

4. **Orchestration**  
   ADF ➔ Triggers notebook + transfers Parquet to Snowflake external stage

5. **ELT in Snowflake**  
   Snowpipe & Streams ➔ Bronze ➔ Silver ➔ Gold with Snowflake Tasks

6. **BI Consumption**  
   Final Gold Tables exposed to Power BI

---

## 📂 Folder Structure
```
/
|-- azure_function_app/              # Python code for event generation
|-- databricks_notebooks/           # Delta write logic (JSON from Avro)
|-- snowflake/
|   |-- bronze.sql
|   |-- silver.sql
|   |-- gold.sql
|   |-- masking_policy.sql
|-- ADF_pipeline_definitions/       # JSON ARM templates
|-- docs/
|   |-- ERD.png
|   |-- architecture.png
```

---

## 🚀 How to Run
1. Deploy Function App ➔ Configure Key Vault and Event Hub
2. Monitor ADLS for Avro ingestion (Event Hub capture)
3. Schedule Databricks notebook ➔ Output to Delta partitioned by `event_date`
4. ADF Pipeline ➔ Triggers notebook ➔ Pushes Parquet to Snowflake stage
5. Resume Snowflake Tasks (Bronze ➔ Silver ➔ Gold)
6. Query with Power BI

---

## 🔒 GDPR Compliance
- `license_plate` column is masked based on user role
- Only roles `TOLL_ADMIN` and `COMPLIANCE_OFFICER` can view unmasked values

---

## 🌟 Highlights
- Reproduces a production-ready data streaming context from scratch
- Demonstrates partitioning, CDC, masking, and dimensional modeling
- Fully modular and easy to expand

---

## 🌐 Hashtags for Sharing
```
#DataEngineering #Snowflake #Azure #DeltaLake #ADF #Databricks #StreamingData #ETL #MedallionArchitecture #Kimball #GDPR #PowerBI #PySpark #EventHub #ModernDataStack
```

