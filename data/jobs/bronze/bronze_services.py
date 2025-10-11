# data/jobs/bronze/bronze_services.py
from pyspark.sql import SparkSession, functions as F

RAW_PATH   = "hdfs://localhost:9000/data/raw/telco/services/Telco_customer_churn_services.csv"
DELTA_PATH = "hdfs:///data/delta/bronze/telco_services"

spark = (
    SparkSession.builder
    .appName("telco_bronze_services")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    # avoid Spark 3 “ancient datetime” Parquet error
    .config("spark.sql.parquet.datetimeRebaseModeInWrite", "CORRECTED")
    .config("spark.sql.parquet.int96RebaseModeInWrite", "CORRECTED")
    .getOrCreate()
)

# quick delimiter guess from first line
first = spark.read.text(RAW_PATH).head()[0]
cands = [",", ";", "\t", "|"]
delim = max(cands, key=lambda d: len(first.split(d)))
print("Using delimiter:", repr(delim))

df = (
    spark.read
    .option("header", True)
    .option("sep", delim)
    .option("quote", '"')
    .option("escape", '"')
    .option("multiLine", True)
    .option("ignoreLeadingWhiteSpace", True)
    .option("ignoreTrailingWhiteSpace", True)
    .option("mode", "PERMISSIVE")
    .option("badRecordsPath", "hdfs:///tmp/badrecords/telco_services")
    .csv(RAW_PATH)                   # no explicit schema
)

# normalize column names a bit (lowercase + underscores)
for c in df.columns:
    df = df.withColumnRenamed(c, "_".join(c.strip().lower().split()))

# guarantee modern load_date to avoid ancient-date write issues
df = df.withColumn("load_date", F.current_date())

print("INPUT ROWS:", df.count())

(df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .save(DELTA_PATH)
)

print("WROTE DELTA TO", DELTA_PATH)
spark.stop()
