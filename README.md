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

## ✅ Output

- Bronze Layer: Raw toll events from Delta files
- Silver Layer: Cleaned events, fraud detection, vehicle profiling
- Gold Layer: Kimball-modeled EDW with fact and dimension tables

---

## 📎 Tags

#DataEngineering #Azure #Snowflake #Databricks #Streaming #EventHub #MedallionArchitecture #DeltaLake #GDPR #RealTimeAnalytics #ADF #KimballModel #TollSystem #Python #PySpark

