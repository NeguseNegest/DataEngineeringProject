# data/jobs/bronze/bronze_services.py
import argparse
from pyspark.sql import SparkSession, functions as F

def spark(app):
    return (
        SparkSession.builder
        .appName(app)
        # Delta Lake integration
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        # HDFS endpoint (from docker-compose: hdfs-namenode on port 9000)
        .config("spark.hadoop.fs.defaultFS", "hdfs://hdfs-namenode:9000")
        # small local-friendly shuffle
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )

def main(ingest_date: str, out_path: str):
    s = spark("bronze-delta-sanity")

    # --- 1) tiny in-memory DataFrame -----------------------------------------
    df = s.createDataFrame(
        [
            ("a001", "Alice", 29),
            ("b002", "Bob",   41),
            ("c003", "Cara",  35),
        ],
        ["customerID", "name", "age"]
    ).withColumn("ingest_date", F.lit(ingest_date))

    # --- 2) write as Delta to HDFS -------------------------------------------
    (df.write
        .format("delta")
        .mode("append")
        .partitionBy("ingest_date")
        .save(out_path))

    print(f"✅ Wrote Delta table → {out_path} (ingest_date={ingest_date})")

    # --- 3) read back & show --------------------------------------------------
    back = s.read.format("delta").load(out_path)
    print("🔎 Read-back sample:")
    back.where(F.col("ingest_date") == ingest_date).show(truncate=False)

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--ingest_date", required=True, help="YYYY-MM-DD")
    ap.add_argument(
        "--out_path",
        default="hdfs://hdfs-namenode:9000/data/delta/bronze/telco/services",
        help="Delta output path on HDFS",
    )
    args = ap.parse_args()
    main(args.ingest_date, args.out_path)
