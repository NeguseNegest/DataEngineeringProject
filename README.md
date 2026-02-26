# DataEngineeringProject

#### Folder structure
```
/data/raw/telco/{demographics,location,population,services,status}/ingest_date=YYYY-MM-DD/
/data/delta/bronze/
/data/delta/silver/
/data/delta/gold/
```

## How to set up everything ( for collaborators)

#### Docker

```
docker compose pull
docker compose up -d


```

#### HDFS folders

``` 
##### Create target layout
docker exec -it hdfs-namenode hdfs dfs -mkdir -p /data/delta/{bronze,silver,gold}/telco

##### Make the 'spark' user (who runs jobs in spark-master) able to write
docker exec -it hdfs-namenode hdfs dfs -chown -R spark:supergroup /data/delta
docker exec -it hdfs-namenode hdfs dfs -chmod -R 775 /data/delta

##### Verify
docker exec -it hdfs-namenode hdfs dfs -ls -R /data/delta

```

##### Check everything is in order

```
# Create a tiny test script inside the spark-master container
docker exec -it spark-master bash -lc 'cat > /tmp/spark_smoke.py << "PY"
from pyspark.sql import SparkSession
spark = (SparkSession.builder
    .appName("spark-smoke")
    .config("spark.sql.extensions","io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog","org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config("spark.hadoop.fs.defaultFS","hdfs://hdfs-namenode:9000")
    .getOrCreate())
print("Spark version:", spark.version, "| count =", spark.range(5).count())
print("OK  Spark up; stopping.")
spark.stop()
PY'

# Submit (includes Delta jars so your session matches real runs)
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
