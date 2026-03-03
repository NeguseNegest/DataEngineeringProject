# DataEngineeringProject  
Customer Churn Analysis with Delta Lake and Apache Spark  
ID2221 – March 2026  

## Project Overview

This project builds an end-to-end lakehouse pipeline for customer churn analysis using the Telco Customer Churn dataset.

The goals of the project are:

1. Design a robust and reproducible data pipeline  
2. Transform raw data into analytics-ready datasets  
3. Train and evaluate a machine learning model to predict churn  

The system is built using Apache Spark, Delta Lake, HDFS, Docker Compose and JupyterLab.

The architecture follows a Bronze to Silver to Gold layered lakehouse design.

## Architecture

### Storage (HDFS)

Raw CSV files are stored in:

```
/data/raw/telco/{demographics,location,population,services,status}/ingest_date=YYYY-MM-DD/
```

Delta tables are stored in:

```
/data/delta/{bronze,silver,gold}/telco/
```

Delta Lake provides ACID transactions and schema enforcement.

### Compute (Spark)

The system includes one Spark master and one Spark worker.

All jobs connect via:

```
spark://spark-master:7077
```

Spark is configured with Delta extensions enabled and HDFS as the default file system.

### Development (JupyterLab)

Jupyter runs inside Docker and is accessible at:

```
http://localhost:8888
```

Each pipeline layer has a notebook for experimentation and a corresponding Python file for production execution using spark-submit.

## Bronze Layer – Raw Ingestion

Each CSV file is schema standardized, column names normalized, data types corrected and missing values analyzed.

Data is written to Delta tables in:

```
/data/delta/bronze/telco/
```

## Silver Layer – Transformation and Standardization

Processing includes removing unnecessary columns, performing type conversions, encoding categorical fields and cleaning inconsistencies.

All tables are written as queryable Delta tables in:

```
/data/delta/silver/telco/
```

## Gold Layer – Feature Engineering and ML Preparation

Steps performed:

- Load all Silver tables  
- Join datasets  
  - Location joined with Population using zip code  
  - Remaining tables joined using customer ID  
- Remove duplicated or unnecessary columns  
- One-hot encode categorical features  
- Prevent data leakage  
- Prepare machine learning ready dataset  

The final dataset is stored in:

```
/data/delta/gold/telco/
```

## Machine Learning

The dataset is split into 70 percent training data and 30 percent test data.

VectorAssembler is used to create the feature vector. StandardScaler is used for normalization. Logistic Regression is used for classification.

### Model Performance

| Metric    | Value    |
|-----------|----------|
| Accuracy  | 0.845042 |
| Precision | 0.726708 |
| Recall    | 0.662264 |
| F1-score  | 0.842661 |
| AUC-ROC   | 0.901225 |

### Confusion Matrix

|            | Pred Neg | Pred Pos |
|------------|----------|----------|
| Actual Neg | 1345     | 132      |
| Actual Pos | 179      | 351      |

The most influential features for churn prediction were number of referrals and tenure in months.

## Setup Instructions for Collaborators

### 1. Start Docker

```bash
docker compose pull
docker compose up -d
```

### 2. Create HDFS Delta Folders

```bash
docker exec -it hdfs-namenode hdfs dfs -mkdir -p /data/delta/{bronze,silver,gold}/telco

docker exec -it hdfs-namenode hdfs dfs -chown -R spark:supergroup /data/delta
docker exec -it hdfs-namenode hdfs dfs -chmod -R 775 /data/delta

docker exec -it hdfs-namenode hdfs dfs -ls -R /data/delta
```

### 3. Verify Spark and Delta Setup

Create test script:

```bash
docker exec -it spark-master bash -lc 'cat > /tmp/spark_smoke.py << "PY"
from pyspark.sql import SparkSession
spark = (SparkSession.builder
    .appName("spark-smoke")
    .config("spark.sql.extensions","io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog","org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config("spark.hadoop.fs.defaultFS","hdfs://hdfs-namenode:9000")
    .getOrCreate())
print("Spark version:", spark.version, "| count =", spark.range(5).count())
print("Spark is running correctly")
spark.stop()
PY'
```

Submit test:

```bash
docker exec -it spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --deploy-mode client \
  --packages io.delta:delta-spark_2.12:3.3.1 \
  --conf spark.jars.ivy=/tmp/.ivy2 \
  --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
  --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
  --conf spark.hadoop.fs.defaultFS=hdfs://hdfs-namenode:9000 \
  /tmp/spark_smoke.py
```

## Running the Pipeline

Run each stage using spark-submit:

```bash
docker exec -it spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --deploy-mode client \
  --packages io.delta:delta-spark_2.12:3.3.1 \
  --conf spark.jars.ivy=/tmp/.ivy2 \
  --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
  --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
  --conf spark.hadoop.fs.defaultFS=hdfs://hdfs-namenode:9000 \
  /opt/project/data/jobs/bronze/bronze_services.py
```

Repeat for:

```
/opt/project/data/jobs/silver/silver_services.py
/opt/project/data/jobs/gold/gold_service.py
```