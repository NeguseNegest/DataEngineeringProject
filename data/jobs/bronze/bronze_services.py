# data/jobs/bronze/bronze_services.py
import argparse
from pyspark.sql import SparkSession, functions as F
import os, re

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

def normalize_token(s: str) -> str:
    s = s.strip().lower()
    s = re.sub(r'[^a-z0-9]+', '_', s)      # non-alnum -> _
    s = re.sub(r'_+', '_', s)               # collapse multiple _
    s = s.strip('_')
    return s

def make_prefix_from_filename(filename: str) -> str:
    base = os.path.splitext(filename)[0]
    # remove literal 'Telco_customer' (case-insensitive), then normalize
    base = re.sub(r'(?i)telco_customer', '', base)
    base = normalize_token(base)
    return base or "file"

def uniquify(names):
    """Ensure names are unique by appending _1, _2 ... when needed."""
    seen = {}
    out = []
    for n in names:
        if n not in seen:
            seen[n] = 0
            out.append(n)
        else:
            seen[n] += 1
            out.append(f"{n}_{seen[n]}")
    return out

RAW_DIR = "/data/raw/telco/telco_local/"
OUT_DIR = "hdfs://hdfs-namenode:9000//data/delta/bronze/"


files = ['Telco_customer_churn_demographics.csv',
         'Telco_customer_churn_location.csv',
         'Telco_customer_churn_population.csv',
         'Telco_customer_churn_status.csv',
         'Telco_customer_churn_services.csv',]
files.sort()

def main(ingest_date: str, out_path: str):
    s = spark("bronze-delta-sanity")

    for f in files:
        path = os.path.join(RAW_DIR, f)
        prefix = make_prefix_from_filename(f)
        print("="*100)
        print(f"📂 Processing: {f}   →   prefix='{prefix}'")
        print("="*100)

        # Read (lazy)
        df = (
            s.read
            .option("header", "true")
            .csv(path)
        )

        # Build rename map
        raw_cols = df.columns
        norm_cols = [normalize_token(c) for c in raw_cols]
        # prepend prefix
        target_cols = [f"{prefix}_{c}" if c else prefix for c in norm_cols]
        # ensure uniqueness (in case different raw columns normalize to same token)
        target_cols = uniquify(target_cols)
        print(target_cols)
        rename_pairs = list(zip(raw_cols, target_cols))
        # Apply renames
        for old, new in rename_pairs:
            if old != new:
                df = df.withColumnRenamed(old, new)

        # Write to Parquet (Spark-friendly)
        out_path = os.path.join(OUT_DIR, prefix)
        (
            df.write
            .mode("overwrite")     # idempotent reruns
            .parquet(out_path)
        )

        print(f"✅ Saved: {out_path}")

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
