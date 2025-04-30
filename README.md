# TollPass Data Streaming Pipeline

This project implements a complete real-time data engineering pipeline that simulates toll event streams, detects fraud, and builds a modern Kimball-style EDW architecture using Azure, Databricks, and Snowflake.

---

## 📌 Overview

This end-to-end pipeline mimics a real-world toll pass streaming system. Events are generated in real-time, processed using Databricks and Azure Functions, stored in Delta format, and ingested into Snowflake where transformations build Bronze, Silver, and Gold layers. Data is masked for GDPR compliance and visualized in BI tools.

---

## 🛠️ Technologies Used

- **Azure**: Event Hub, Blob Storage, Data Lake Gen2, Data Factory, Key Vault, Functions
- **Snowflake**: Streams, Tasks, Masking Policies, Medallion Architecture
- **Databricks**: Delta Lake, PySpark, AVRO-to-Delta conversion
- **Languages**: Python, SQL
- **Security**: Azure Key Vault, RBAC, Data Masking (GDPR)

---

## 📂 Project Structure

```bash
📁 azure-function-app
   └── function_app.py        # Generates mock toll data and streams to Event Hub
📁 databricks-notebooks
   └── convert_avro_to_delta.py  # Converts AVRO to Delta & adds schema-on-read
📁 snowflake-scripts
   ├── bronze_layer.sql
   ├── silver_layer.sql
   ├── gold_layer.sql
   ├── masking_policies.sql
   └── orchestration_tasks.sql
📁 documentation
   └── architecture_diagram.png
📄 README.md (this file)
```

---

## 🔄 Pipeline Execution Flow

1. **Azure Function App** generates real-time JSON toll events and pushes them to Event Hub.
2. **Databricks** reads AVRO files from ADLS Gen2 → parses them to Delta format → partitions by `event_date`.
3. **Azure Data Factory (ADF)** orchestrates Databricks jobs and transfers data to Blob for Snowflake ingestion.
4. **Snowflake** ingests data into Bronze layer → transforms it into Silver → aggregates into Gold EDW.
5. **Data Masking** policies protect sensitive PII like license plates.
6. **Power BI/Tableau** consumes the data from the EDW for visualization.

---

## 🔐 GDPR-Compliant Masking

A masking policy is applied on the `license_plate` column in Snowflake to obfuscate data for unauthorized roles:

```sql
CREATE OR REPLACE MASKING POLICY mask_license_plate_gdpr AS
  (val STRING) RETURNS STRING ->
  CASE
    WHEN CURRENT_ROLE() IN ('COMPLIANCE_OFFICER', 'TOLL_ADMIN') THEN val
    ELSE CONCAT(SUBSTR(val, 1, 3), '****')
  END;
```

---

## 📊 Azure Function (Mock Streaming Generator)

```python
import azure.functions as func
import json, random, datetime, logging
from azure.identity import DefaultAzureCredential
from azure.eventhub import EventHubProducerClient, EventData
from azure.keyvault.secrets import SecretClient

app = func.FunctionApp()

@app.function_name(name="generate_toll_events")
@app.timer_trigger(schedule="0 */1 * * * *", arg_name="myTimer", run_on_startup=True, use_monitor=False)
def generate_toll_events(myTimer: func.TimerRequest) -> None:
    credential = DefaultAzureCredential()
    key_vault_url = "<YOUR-KEY-VAULT-URL>"
    secret_client = SecretClient(vault_url=key_vault_url, credential=credential)

    event_hub_name = secret_client.get_secret("event-hub-name-stream").value
    event_hub_namespace = secret_client.get_secret("event-hub-namespace-stream").value

    producer = EventHubProducerClient(
        fully_qualified_namespace=event_hub_namespace,
        eventhub_name=event_hub_name,
        credential=credential
    )

    toll_event = {
        "transaction_id": random.randint(100000, 999999),
        "vehicle_id": f"VEH{random.randint(1000, 9999)}",
        "license_plate": f"XYZ-{random.randint(1000, 9999)}",
        "vehicle_type": random.choice(["Car", "Truck"]),
        "toll_booth_id": "TB202",
        "entry_point": "North Gate",
        "exit_point": "West Exit",
        "timestamp": datetime.datetime.utcnow().isoformat(),
        "payment_method": random.choice(["TAG", "CASH", "CARD", "FRAUD"]),
        "toll_amount": round(random.uniform(3.0, 12.0), 2),
        "tag_valid": random.choice([True, False]),
        "fraud_detected": False
    }

    if toll_event["payment_method"] == "FRAUD" or (toll_event["payment_method"] == "TAG" and not toll_event["tag_valid"]):
        toll_event["fraud_detected"] = True

    batch = producer.create_batch()
    batch.add(EventData(json.dumps(toll_event)))
    producer.send_batch(batch)
```

---

## 🔁 Databricks AVRO → Delta Conversion

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, to_date, from_json
from pyspark.sql.types import *

spark = SparkSession.builder.appName("TollEventAvroToDelta").getOrCreate()

schema = StructType([
    StructField("transaction_id", IntegerType(), True),
    StructField("vehicle_id", StringType(), True),
    StructField("license_plate", StringType(), True),
    StructField("vehicle_type", StringType(), True),
    StructField("toll_booth_id", StringType(), True),
    StructField("entry_point", StringType(), True),
    StructField("exit_point", StringType(), True),
    StructField("timestamp", StringType(), True),
    StructField("payment_method", StringType(), True),
    StructField("toll_amount", DoubleType(), True),
    StructField("tag_valid", BooleanType(), True),
    StructField("fraud_detected", BooleanType(), True)
])

input_path = "abfss://capture@<storage>.dfs.core.windows.net/path/to/*.avro"
output_path = "abfss://delta@<storage>.dfs.core.windows.net/path/to/processed/"

raw_df = spark.read.format("avro").load(input_path)
parsed_df = raw_df.withColumn("body_str", col("Body").cast("string")).withColumn("parsed", from_json("body_str", schema)).select("parsed.*")

df = parsed_df.withColumn("event_date", to_date(to_timestamp("timestamp")))

df.write.format("delta").mode("append").partitionBy("event_date").save(output_path)
```

---

## SNOWFLAKE - BRONZE + SILVER LAYERS
```SQL
-- =========================
-- DATABASE & WAREHOUSE
-- =========================
CREATE DATABASE IF NOT EXISTS TOLLPASS_DBO;
USE DATABASE TOLLPASS_DBO;

CREATE WAREHOUSE IF NOT EXISTS COMPUTE_WH;
USE WAREHOUSE COMPUTE_WH;

-- =========================
-- STORAGE & NOTIFICATION INTEGRATIONS
-- =========================
CREATE OR REPLACE STORAGE INTEGRATION azure_int_tollpass
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = AZURE
  ENABLED = TRUE
  AZURE_TENANT_ID = 'cc660712-643a-4061-994d-5d82e0269523'
  STORAGE_ALLOWED_LOCATIONS = ('');

CREATE OR REPLACE NOTIFICATION INTEGRATION azure_blob_notify
  TYPE = QUEUE
  NOTIFICATION_PROVIDER = AZURE_STORAGE_QUEUE
  ENABLED = TRUE
  AZURE_STORAGE_QUEUE_PRIMARY_URI = ''
  AZURE_TENANT_ID = '';

-- =========================
-- BRONZE LAYER
-- =========================
CREATE OR REPLACE SCHEMA BRONZE_LAYER;

CREATE OR REPLACE TABLE BRONZE_LAYER.STG_TOLL_EVENTS (
    transaction_id NUMBER,
    vehicle_id STRING,
    license_plate STRING,
    vehicle_type STRING,
    toll_booth_id STRING,
    entry_point STRING,
    exit_point STRING,
    timestamp STRING,
    payment_method STRING,
    toll_amount FLOAT,
    tag_valid BOOLEAN,
    fraud_detected BOOLEAN
);

CREATE OR REPLACE STAGE stage_toll_events_ingest
URL = ''
STORAGE_INTEGRATION = azure_int_tollpass
FILE_FORMAT = (TYPE = PARQUET);

CREATE OR REPLACE PIPE pipe_toll_events_ingest
AUTO_INGEST = TRUE
INTEGRATION = azure_blob_notify
AS
COPY INTO BRONZE_LAYER.STG_TOLL_EVENTS
FROM @stage_toll_events_ingest
FILE_FORMAT = (TYPE = PARQUET)
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;

CREATE OR REPLACE STREAM BRONZE_LAYER.STRM_STG_TOLL_EVENTS
ON TABLE BRONZE_LAYER.STG_TOLL_EVENTS
APPEND_ONLY = FALSE;

-- Fallback COPY Task (in case Snowpipe fails)
CREATE OR REPLACE TASK BRONZE_LAYER.task_fallback_ingest_bronze
WAREHOUSE = COMPUTE_WH
SCHEDULE = '1 MINUTE'
AS
COPY INTO BRONZE_LAYER.STG_TOLL_EVENTS
FROM @stage_toll_events_ingest
FILE_FORMAT = (TYPE = PARQUET)
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;

-- =========================
-- SILVER LAYER
-- =========================
CREATE OR REPLACE SCHEMA SILVER_LAYER;

CREATE OR REPLACE TABLE SILVER_LAYER.SLVR_TOLL_EVENTS (
    transaction_id NUMBER,
    vehicle_id STRING,
    license_plate STRING,
    vehicle_type STRING,
    toll_booth_id STRING,
    entry_point STRING,
    exit_point STRING,
    timestamp TIMESTAMP_NTZ,
    payment_method STRING,
    toll_amount FLOAT,
    tag_valid BOOLEAN,
    fraud_detected BOOLEAN,
    event_date DATE
) CLUSTER BY (event_date);

CREATE OR REPLACE TABLE SILVER_LAYER.SLVR_FRAUD_EVENTS (
    transaction_id NUMBER,
    vehicle_id STRING,
    license_plate STRING,
    fraud_detected BOOLEAN,
    fraud_type STRING,
    detected_timestamp TIMESTAMP_NTZ,
    event_date DATE
) CLUSTER BY (event_date);

CREATE OR REPLACE TABLE SILVER_LAYER.SLVR_VEHICLE_PROFILES (
    vehicle_id STRING PRIMARY KEY,
    first_license_plate STRING,
    first_seen_type STRING,
    first_entry_point STRING,
    first_event_date DATE
) CLUSTER BY (first_event_date);

-- =========================
-- FLAT INDEPENDENT TASKS (No DAG)
-- =========================

CREATE OR REPLACE TASK SILVER_LAYER.task_insert_slvr_toll_events
WAREHOUSE = COMPUTE_WH
SCHEDULE = '1 MINUTE'
WHEN SYSTEM$STREAM_HAS_DATA('BRONZE_LAYER.STRM_STG_TOLL_EVENTS')
AS
INSERT INTO SILVER_LAYER.SLVR_TOLL_EVENTS
SELECT
    transaction_id,
    vehicle_id,
    license_plate,
    vehicle_type,
    toll_booth_id,
    entry_point,
    exit_point,
    TRY_TO_TIMESTAMP_NTZ(timestamp),
    payment_method,
    toll_amount,
    tag_valid,
    fraud_detected,
    CAST(TRY_TO_TIMESTAMP_NTZ(timestamp) AS DATE)
FROM BRONZE_LAYER.STRM_STG_TOLL_EVENTS;

CREATE OR REPLACE TASK SILVER_LAYER.task_insert_fraud_events
WAREHOUSE = COMPUTE_WH
SCHEDULE = '2 MINUTE'
AS
INSERT INTO SILVER_LAYER.SLVR_FRAUD_EVENTS
SELECT
    transaction_id,
    vehicle_id,
    license_plate,
    fraud_detected,
    CASE WHEN fraud_detected THEN 'Suspected Fraud' ELSE 'Legit' END,
    timestamp,
    event_date
FROM SILVER_LAYER.SLVR_TOLL_EVENTS
WHERE fraud_detected = TRUE;


CREATE OR REPLACE TASK SILVER_LAYER.task_insert_vehicle_profiles
WAREHOUSE = COMPUTE_WH
SCHEDULE = '3 MINUTE'
AS
INSERT INTO SILVER_LAYER.SLVR_VEHICLE_PROFILES
SELECT
    vehicle_id,
    license_plate AS first_license_plate,
    vehicle_type AS first_seen_type,
    entry_point AS first_entry_point,
    CAST(timestamp AS DATE) AS first_event_date
FROM (
    SELECT
        vehicle_id,
        license_plate,
        vehicle_type,
        entry_point,
        timestamp,
        ROW_NUMBER() OVER (
            PARTITION BY vehicle_id
            ORDER BY timestamp
        ) AS rn
    FROM SILVER_LAYER.SLVR_TOLL_EVENTS
) ranked
WHERE rn = 1;


-- =========================
-- GDPR MASKING
-- =========================
CREATE ROLE IF NOT EXISTS COMPLIANCE_OFFICER;
CREATE ROLE IF NOT EXISTS TOLL_ADMIN;

GRANT USAGE ON DATABASE TOLLPASS_DBO TO ROLE COMPLIANCE_OFFICER;
GRANT USAGE ON DATABASE TOLLPASS_DBO TO ROLE TOLL_ADMIN;

GRANT USAGE ON SCHEMA TOLLPASS_DBO.SILVER_LAYER TO ROLE COMPLIANCE_OFFICER;
GRANT USAGE ON SCHEMA TOLLPASS_DBO.SILVER_LAYER TO ROLE TOLL_ADMIN;

CREATE OR REPLACE MASKING POLICY mask_license_plate_gdpr AS
  (license_plate STRING) 
  RETURNS STRING ->
  CASE
    WHEN CURRENT_ROLE() IN ('COMPLIANCE_OFFICER', 'TOLL_ADMIN') THEN license_plate
    ELSE CONCAT(SUBSTR(license_plate, 1, 3), '****')
  END;

ALTER TABLE TOLLPASS_DBO.SILVER_LAYER.SLVR_TOLL_EVENTS
  MODIFY COLUMN license_plate
  SET MASKING POLICY mask_license_plate_gdpr;

-- =========================
-- ACTIVATE TASKS
-- =========================
ALTER TASK BRONZE_LAYER.task_fallback_ingest_bronze RESUME;
ALTER TASK SILVER_LAYER.task_insert_slvr_toll_events RESUME;
ALTER TASK SILVER_LAYER.task_insert_fraud_events RESUME;
ALTER TASK SILVER_LAYER.task_insert_vehicle_profiles RESUME;


-- =========================
-- VALIDATION
-- =========================
SHOW TASKS;
SHOW TASKS IN SCHEMA BRONZE_LAYER;
SHOW TASKS IN SCHEMA SILVER_LAYER;

SELECT * FROM SILVER_LAYER.SLVR_TOLL_EVENTS LIMIT 10;
SELECT * FROM SILVER_LAYER.SLVR_FRAUD_EVENTS LIMIT 10;
SELECT * FROM SILVER_LAYER.SLVR_VEHICLE_PROFILES LIMIT 10;
```

## SNOWFLAKE - GOLD LAYER - EDW
```SQL
-- =========================
-- DATABASE & WAREHOUSE
-- =========================
CREATE DATABASE IF NOT EXISTS TOLLPASS_DBO;
USE DATABASE TOLLPASS_DBO;

CREATE WAREHOUSE IF NOT EXISTS COMPUTE_WH;
USE WAREHOUSE COMPUTE_WH;

-- =========================
-- SUSPEND EXISTING GOLD TASKS
-- =========================
ALTER TASK IF EXISTS task_fact_toll_insert SUSPEND;
ALTER TASK IF EXISTS task_mini_dim_vehicle SUSPEND;
ALTER TASK IF EXISTS task_mini_dim_location SUSPEND;
ALTER TASK IF EXISTS task_dim_vehicle_hist SUSPEND;
ALTER TASK IF EXISTS task_dim_junk SUSPEND;
ALTER TASK IF EXISTS task_dim_toll_location SUSPEND;
ALTER TASK IF EXISTS task_dim_time SUSPEND;

-- =========================
-- GOLD SCHEMA & TABLES
-- =========================
CREATE OR REPLACE SCHEMA GOLD_LAYER;

CREATE OR REPLACE TABLE GOLD_LAYER.DIM_TOLL_JUNK (
    junk_id NUMBER PRIMARY KEY AUTOINCREMENT,
    payment_method STRING,
    tag_valid BOOLEAN
);

CREATE OR REPLACE TABLE GOLD_LAYER.DIM_VEHICLE_HIST (
    vehicle_sk NUMBER PRIMARY KEY AUTOINCREMENT,
    vehicle_id STRING,
    license_plate STRING,
    vehicle_type STRING,
    entry_point STRING,
    start_date DATE,
    end_date DATE,
    current_flag BOOLEAN,
    version NUMBER
);

CREATE OR REPLACE TABLE GOLD_LAYER.DIM_TOLL_LOCATION (
    location_sk NUMBER PRIMARY KEY AUTOINCREMENT,
    toll_booth_id STRING,
    entry_point STRING,
    exit_point STRING,
    location_type STRING
);

CREATE OR REPLACE TABLE GOLD_LAYER.DIM_TIME (
    time_sk NUMBER PRIMARY KEY AUTOINCREMENT,
    full_timestamp TIMESTAMP_NTZ,
    event_date DATE,
    hour NUMBER,
    day NUMBER,
    month NUMBER,
    year NUMBER
);

CREATE OR REPLACE TABLE GOLD_LAYER.MINI_DIM_LOCATION (
    mini_location_sk NUMBER PRIMARY KEY AUTOINCREMENT,
    entry_point STRING,
    exit_point STRING,
    location_type STRING,
    hash_key STRING
);

CREATE OR REPLACE TABLE GOLD_LAYER.MINI_DIM_VEHICLE (
    mini_vehicle_sk NUMBER PRIMARY KEY AUTOINCREMENT,
    vehicle_type STRING,
    entry_point STRING,
    hash_key STRING
);

CREATE OR REPLACE TABLE GOLD_LAYER.FACT_TOLL_PASS (
    toll_pass_id NUMBER PRIMARY KEY AUTOINCREMENT,
    transaction_id NUMBER,
    vehicle_sk NUMBER,
    location_sk NUMBER,
    mini_location_sk NUMBER,
    mini_vehicle_sk NUMBER,
    entry_time_sk NUMBER,
    exit_time_sk NUMBER,
    junk_id NUMBER,
    toll_amount FLOAT,
    fraud_detected BOOLEAN
);

-- =========================
-- STREAMS FOR CDC
-- =========================
CREATE OR REPLACE STREAM STRM_SLVR_TOLL_EVENTS ON TABLE SILVER_LAYER.SLVR_TOLL_EVENTS;
CREATE OR REPLACE STREAM STRM_SLVR_FRAUD_EVENTS ON TABLE SILVER_LAYER.SLVR_FRAUD_EVENTS;

-- =========================
-- FLAT TASKS (NO DAG)
-- =========================

CREATE OR REPLACE TASK task_dim_time
WAREHOUSE = COMPUTE_WH
SCHEDULE = '1 MINUTE'
AS
MERGE INTO GOLD_LAYER.DIM_TIME t
USING (
    SELECT DISTINCT
        timestamp AS full_timestamp,
        CAST(timestamp AS DATE) AS event_date,
        EXTRACT(HOUR FROM timestamp) AS hour,
        EXTRACT(DAY FROM timestamp) AS day,
        EXTRACT(MONTH FROM timestamp) AS month,
        EXTRACT(YEAR FROM timestamp) AS year
    FROM SILVER_LAYER.SLVR_TOLL_EVENTS
) s
ON t.full_timestamp = s.full_timestamp
WHEN NOT MATCHED THEN
INSERT (full_timestamp, event_date, hour, day, month, year)
VALUES (s.full_timestamp, s.event_date, s.hour, s.day, s.month, s.year);

CREATE OR REPLACE TASK task_dim_toll_location
WAREHOUSE = COMPUTE_WH
SCHEDULE = '1 MINUTE'
AS
MERGE INTO GOLD_LAYER.DIM_TOLL_LOCATION t
USING (
    SELECT DISTINCT
        toll_booth_id,
        entry_point,
        exit_point,
        'entry_exit' AS location_type
    FROM SILVER_LAYER.SLVR_TOLL_EVENTS
) s
ON t.toll_booth_id = s.toll_booth_id
WHEN NOT MATCHED THEN
INSERT (toll_booth_id, entry_point, exit_point, location_type)
VALUES (s.toll_booth_id, s.entry_point, s.exit_point, s.location_type);

CREATE OR REPLACE TASK task_dim_junk
WAREHOUSE = COMPUTE_WH
SCHEDULE = '1 MINUTE'
AS
MERGE INTO GOLD_LAYER.DIM_TOLL_JUNK t
USING (
    SELECT DISTINCT payment_method, tag_valid
    FROM SILVER_LAYER.SLVR_TOLL_EVENTS
) s
ON t.payment_method = s.payment_method AND t.tag_valid = s.tag_valid
WHEN NOT MATCHED THEN
INSERT (payment_method, tag_valid)
VALUES (s.payment_method, s.tag_valid);

CREATE OR REPLACE TASK task_dim_vehicle_hist
WAREHOUSE = COMPUTE_WH
SCHEDULE = '2 MINUTE'
AS
INSERT INTO GOLD_LAYER.DIM_VEHICLE_HIST (
    vehicle_id,
    license_plate,
    vehicle_type,
    entry_point,
    start_date,
    end_date,
    current_flag,
    version
)
SELECT
    vehicle_id,
    license_plate,
    vehicle_type,
    entry_point,
    CAST(timestamp AS DATE) AS start_date,
    NULL AS end_date,
    TRUE AS current_flag,
    1 AS version
FROM SILVER_LAYER.SLVR_TOLL_EVENTS src
WHERE NOT EXISTS (
    SELECT 1 FROM GOLD_LAYER.DIM_VEHICLE_HIST tgt
    WHERE tgt.vehicle_id = src.vehicle_id AND tgt.current_flag = TRUE
);

CREATE OR REPLACE TASK task_mini_dim_location
WAREHOUSE = COMPUTE_WH
SCHEDULE = '2 MINUTE'
AS
MERGE INTO GOLD_LAYER.MINI_DIM_LOCATION m
USING (
    SELECT DISTINCT
        entry_point,
        exit_point,
        location_type,
        MD5(TO_VARCHAR(entry_point) || ':' || TO_VARCHAR(exit_point)) AS hash_key
    FROM GOLD_LAYER.DIM_TOLL_LOCATION
) s
ON m.hash_key = s.hash_key
WHEN NOT MATCHED THEN
INSERT (entry_point, exit_point, location_type, hash_key)
VALUES (s.entry_point, s.exit_point, s.location_type, s.hash_key);

CREATE OR REPLACE TASK task_mini_dim_vehicle
WAREHOUSE = COMPUTE_WH
SCHEDULE = '3 MINUTE'
AS
MERGE INTO GOLD_LAYER.MINI_DIM_VEHICLE m
USING (
    SELECT DISTINCT
        vehicle_type,
        entry_point,
        MD5(vehicle_type || ':' || entry_point) AS hash_key
    FROM GOLD_LAYER.DIM_VEHICLE_HIST
    WHERE current_flag = TRUE
) s
ON m.hash_key = s.hash_key
WHEN NOT MATCHED THEN
INSERT (vehicle_type, entry_point, hash_key)
VALUES (s.vehicle_type, s.entry_point, s.hash_key);

CREATE OR REPLACE TASK task_fact_toll_insert
WAREHOUSE = COMPUTE_WH
SCHEDULE = '1 MINUTE'
AS
INSERT INTO GOLD_LAYER.FACT_TOLL_PASS (
    transaction_id,
    vehicle_sk,
    location_sk,
    mini_location_sk,
    mini_vehicle_sk,
    entry_time_sk,
    exit_time_sk,
    junk_id,
    toll_amount,
    fraud_detected
)
SELECT
    t.transaction_id,
    v.vehicle_sk,
    l.location_sk,
    mini_l.mini_location_sk,
    mini_v.mini_vehicle_sk,
    dt.time_sk AS entry_time_sk,
    dt.time_sk AS exit_time_sk,
    j.junk_id,
    t.toll_amount,
    t.fraud_detected
FROM SILVER_LAYER.SLVR_TOLL_EVENTS t
JOIN GOLD_LAYER.DIM_VEHICLE_HIST v
  ON t.vehicle_id = v.vehicle_id AND v.current_flag = TRUE
JOIN GOLD_LAYER.DIM_TOLL_LOCATION l
  ON t.toll_booth_id = l.toll_booth_id
JOIN GOLD_LAYER.MINI_DIM_LOCATION mini_l
  ON mini_l.hash_key = MD5(TO_VARCHAR(t.entry_point) || ':' || TO_VARCHAR(t.exit_point))
JOIN GOLD_LAYER.MINI_DIM_VEHICLE mini_v
  ON mini_v.hash_key = MD5(TO_VARCHAR(t.vehicle_type) || ':' || TO_VARCHAR(t.entry_point))
JOIN GOLD_LAYER.DIM_TOLL_JUNK j
  ON t.payment_method = j.payment_method AND t.tag_valid = j.tag_valid
JOIN GOLD_LAYER.DIM_TIME dt
  ON CAST(t.timestamp AS TIMESTAMP_NTZ) = dt.full_timestamp;
  
-- =========================
-- RESUME TASKS
-- =========================
ALTER TASK task_dim_time RESUME;
ALTER TASK task_dim_toll_location RESUME;
ALTER TASK task_dim_junk RESUME;
ALTER TASK task_dim_vehicle_hist RESUME;
ALTER TASK task_mini_dim_location RESUME;
ALTER TASK task_mini_dim_vehicle RESUME;
ALTER TASK task_fact_toll_insert RESUME;

SELECT * FROM GOLD_LAYER.FACT_TOLL_PASS LIMIT 10;

EXECUTE TASK task_dim_time;
EXECUTE TASK task_dim_toll_location;
EXECUTE TASK task_dim_junk;
EXECUTE TASK task_dim_vehicle_hist;
EXECUTE TASK task_mini_dim_location;
EXECUTE TASK task_mini_dim_vehicle;
EXECUTE TASK task_fact_toll_insert;
```
## ✅ Output

- Bronze Layer: Raw toll events from Delta files
- Silver Layer: Cleaned events, fraud detection, vehicle profiling
- Gold Layer: Kimball-modeled EDW with fact and dimension tables

---

## 📎 Tags

#DataEngineering #Azure #Snowflake #Databricks #Streaming #EventHub #MedallionArchitecture #DeltaLake #GDPR #RealTimeAnalytics #ADF #KimballModel #TollSystem #Python #PySpark

