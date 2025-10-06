import os, re
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Spark
spark = (
    SparkSession.builder
    .appName("rename-and-save-1")
    .getOrCreate()
)

RAW_DIR = "data/raw/0_raw"
OUT_DIR = "data/raw/1_bronze/"

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

# Ensure output root exists (Spark will create subdirs)
os.makedirs(OUT_DIR, exist_ok=True)

files = [f for f in os.listdir(RAW_DIR) if f.endswith(".csv")]
files.sort()

for f in files:
    path = os.path.join(RAW_DIR, f)
    prefix = make_prefix_from_filename(f)
    print("="*100)
    print(f"📂 Processing: {f}   →   prefix='{prefix}'")
    print("="*100)

    # Read (lazy)
    df = (
        spark.read
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