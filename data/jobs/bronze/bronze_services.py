# bronze_services.py (top part)
from pyspark.sql import SparkSession, functions as F
import argparse

from common.schemas import services_schema

parser = argparse.ArgumentParser()
parser.add_argument("--ingest_date", required=True)
parser.add_argument("--commit", action="store_true")
parser.add_argument("--target", default="hdfs://hdfs-namenode:9000/data/delta/bronze/telco/services")
args = parser.parse_args()

builder = (
    SparkSession.builder
      .appName("bronze-services")
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
)

# If you want to run locally, uncomment:
# builder = builder.master("local[*]")

spark = builder.getOrCreate()

from pyspark.sql import functions as F

# 1) Read with explicit schema and delimiter
df_raw = (
    spark.read.format("csv")
      .option("header", "true")
      .option("sep", ";")                     # important for your file
      .option("mode", "PERMISSIVE")
      .option("columnNameOfCorruptRecord", "_corrupt")
      .schema(services_schema)
      .load("hdfs://hdfs-namenode:9000/data/raw/telco/telco_local/Telco_customer_churn_services.csv")
)

# 2) Helpers
def yn_to_bool(col):
    return (F.lower(F.trim(col)).isin("y","yes","true","t","1")).cast("boolean")

comma_to_dot = F.udf(lambda s: s.replace(',', '.') if s is not None else None)

# 3) Clean booleans (Yes/No → True/False)
bool_cols = [
    "Referred a Friend","Phone Service","Multiple Lines","Online Security","Online Backup",
    "Device Protection Plan","Premium Tech Support","Streaming TV","Streaming Movies",
    "Streaming Music","Unlimited Data","Paperless Billing"
]

df_clean = df_raw
for c in bool_cols:
    df_clean = df_clean.withColumn(c, yn_to_bool(F.col(c)))

# 4) Clean numeric strings with comma decimals, then cast
num_str_cols = [
    "Avg Monthly Long Distance Charges","Monthly Charge","Total Charges",
    "Total Refunds","Total Long Distance Charges"
]
for c in num_str_cols:
    df_clean = df_clean.withColumn(c, comma_to_dot(F.col(c)).cast("double"))

# 5) Rename to snake_case
rename_map = {
    "Customer ID":"customer_id",
    "Count":"count",
    "Quarter":"quarter",
    "Referred a Friend":"referred_a_friend",
    "Number of Referrals":"number_of_referrals",
    "Tenure in Months":"tenure_in_months",
    "Offer":"offer",
    "Phone Service":"phone_service",
    "Avg Monthly Long Distance Charges":"avg_monthly_long_distance_charges",
    "Multiple Lines":"multiple_lines",
    "Internet Service":"internet_service",
    "Internet Type":"internet_type",
    "Avg Monthly GB Download":"avg_monthly_gb_download",
    "Online Security":"online_security",
    "Online Backup":"online_backup",
    "Device Protection Plan":"device_protection_plan",
    "Premium Tech Support":"premium_tech_support",
    "Streaming TV":"streaming_tv",
    "Streaming Movies":"streaming_movies",
    "Streaming Music":"streaming_music",
    "Unlimited Data":"unlimited_data",
    "Contract":"contract",
    "Paperless Billing":"paperless_billing",
    "Payment Method":"payment_method",
    "Monthly Charge":"monthly_charge",
    "Total Charges":"total_charges",
    "Total Refunds":"total_refunds",
    "Total Extra Data Charges":"total_extra_data_charges",
    "Total Long Distance Charges":"total_long_distance_charges",
}
for old, new in rename_map.items():
    df_clean = df_clean.withColumnRenamed(old, new)

# 6) Add partition column
df_clean = df_clean.withColumn("ingest_date", F.lit(args.ingest_date))

# 7) Dry-run vs commit
if args.commit:
    (df_clean.write.format("delta")
        .mode("append")
        .partitionBy("ingest_date")
        .save(args.target))
    print(f"✅ Wrote {df_clean.count()} rows to {args.target} (ingest_date={args.ingest_date})")
else:
    df_clean.show(10, truncate=False)
    df_clean.printSchema()
    print("row_count=", df_clean.count())
    df_clean.select("contract").distinct().show()
    df_clean.select("internet_type").distinct().show()
