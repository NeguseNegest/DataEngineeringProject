import os, re
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, when, lower, trim

# Spark
spark = (
    SparkSession.builder
    .appName("cast-to-int-binary")
    .getOrCreate()
)

RAW_DIR = "data/raw/1_bronze"
OUT_DIR = "data/raw/2_bronze_cast_to_int_binary/"

# Ensure output root exists (Spark will create subdirs)
os.makedirs(OUT_DIR, exist_ok=True)

files = [f for f in os.listdir(RAW_DIR) if not f.startswith(".") or f.endswith(".md") ]
files.remove("README.md")
files.sort()

def to_binary(df: DataFrame, col_name: str) -> DataFrame:
    """
    Converts Yes/No, Male/Female, True/False style columns to 1/0 integers.
    """
    df = df.withColumn(
        col_name,
        when(lower(trim(col(col_name))).isin("yes", "y", "true", "1", "male", "m"), 1)
        .when(lower(trim(col(col_name))).isin("no", "n", "false", "0", "female", "f"), 0)
        .otherwise(None)
    )
    return df

def to_int(df: DataFrame, col_name: str) -> DataFrame:
    """
    Trims spaces and casts column to Integer safely.
    """
    df = df.withColumn(
        col_name,
        when(trim(col(col_name)) == "", None)
        .otherwise(col(col_name).cast("int"))
    )
    return df

def to_string(df: DataFrame, col_name: str) -> DataFrame:
    """
    Trims and keeps as string.
    """
    df = df.withColumn(col_name, trim(col(col_name)).cast("string"))
    return df



def apply_churn_transformations(df: DataFrame) -> DataFrame:
    """
    Applies all schema-specific transformations for churn_demographics dataset.
    """
    transformations = {
        # ----- Demographics -----
        "churn_demographics_customer_id": to_string,
        "churn_demographics_count": to_int,
        "churn_demographics_gender": to_binary,
        "churn_demographics_age": to_int,
        "churn_demographics_under_30": to_binary,
        "churn_demographics_senior_citizen": to_binary,
        "churn_demographics_married": to_binary,
        "churn_demographics_dependents": to_binary,
        "churn_demographics_number_of_dependents": to_int,

        # ----- Location -----
        "churn_location_customer_id": to_string,
        "churn_location_count": to_int,
        "churn_location_country": to_string,
        "churn_location_state": to_string,
        "churn_location_city": to_string,
        "churn_location_zip_code": to_string,
        "churn_location_latitude": to_double,
        "churn_location_longitude": to_double,

        # ----- Services -----
        "churn_services_customer_id": to_string,
        "churn_services_count": to_int,
        "churn_services_quarter": to_string,
        "churn_services_referred_a_friend": to_binary,
        "churn_services_number_of_referrals": to_int,
        "churn_services_tenure_in_months": to_int,
        "churn_services_offer": to_string,
        "churn_services_phone_service": to_binary,
        "churn_services_avg_monthly_long_distance_charges": to_double,
        "churn_services_multiple_lines": to_binary,
        "churn_services_internet_service": to_binary,
        "churn_services_internet_type": to_string,
        "churn_services_avg_monthly_gb_download": to_double,
        "churn_services_online_security": to_binary,
        "churn_services_online_backup": to_binary,
        "churn_services_device_protection_plan": to_binary,
        "churn_services_premium_tech_support": to_binary,
        "churn_services_streaming_tv": to_binary,
        "churn_services_streaming_movies": to_binary,
        "churn_services_streaming_music": to_binary,
        "churn_services_unlimited_data": to_binary,
        "churn_services_contract": to_string,
        "churn_services_paperless_billing": to_binary,
        "churn_services_payment_method": to_string,
        "churn_services_monthly_charge": to_double,
        "churn_services_total_charges": to_double,
        "churn_services_total_refunds": to_double,
        "churn_services_total_extra_data_charges": to_double,
        "churn_services_total_long_distance_charges": to_double,
        "churn_services_total_revenue": to_double,

        # ----- Status -----
        "churn_status_customer_id": to_string,
        "churn_status_count": to_int,
        "churn_status_quarter": to_string,
        "churn_status_satisfaction_score": to_int,
        "churn_status_customer_status": to_string,
        "churn_status_churn_label": to_binary,
        "churn_status_churn_value": to_int,
        "churn_status_churn_score": to_int,
        "churn_status_cltv": to_double,
        "churn_status_churn_category": to_string,
        "churn_status_churn_reason": to_string,

        # ----- Population -----
        "churn_population_id": to_string,
        "churn_population_zip_code": to_string,
        "churn_population_population": to_int,

         # ----- Main Churn -----
        "churn_customerid": to_string,
        "churn_count": to_int,
        "churn_country": to_string,
        "churn_state": to_string,
        "churn_city": to_string,
        "churn_zip_code": to_string,
        "churn_lat_long": to_double,  # or split_latlong if you want separate lat/lon
        "churn_latitude": to_double,
        "churn_longitude": to_double,
        "churn_gender": to_binary,
        "churn_senior_citizen": to_binary,
        "churn_partner": to_binary,
        "churn_dependents": to_binary,
        "churn_tenure_months": to_int,
        "churn_phone_service": to_binary,
        "churn_multiple_lines": to_binary,
        "churn_internet_service": to_string,
        "churn_online_security": to_binary,
        "churn_online_backup": to_binary,
        "churn_device_protection": to_binary,
        "churn_tech_support": to_binary,
        "churn_streaming_tv": to_binary,
        "churn_streaming_movies": to_binary,
        "churn_contract": to_string,
        "churn_paperless_billing": to_binary,
        "churn_payment_method": to_string,
        "churn_monthly_charges": to_double,
        "churn_total_charges": to_double,
        "churn_churn_label": to_binary,
        "churn_churn_value": to_int,
        "churn_churn_score": to_int,
        "churn_cltv": to_double,
        "churn_churn_reason": to_string,
    }
    
    for col_name, func in transformations.items():
        if col_name in df.columns:
            df = func(df, col_name)
    
    return df


def to_double(df: DataFrame, c: str) -> DataFrame:
    """Safely cast to double."""
    return df.withColumn(c, when(trim(col(c)) == "", None).otherwise(col(c).cast("double")))

print(files)

for f in files:
    path = os.path.join(RAW_DIR, f)
    print("="*100)
    print(f"📂 Processing: {f}")
    print("="*100)

    # Read (lazy)
    df = (
        spark.read.parquet(path)
    )

    # Apply transformations based on dataset type
    transformedDf = apply_churn_transformations(df)
    # Write (lazy)
    out_path = os.path.join(OUT_DIR, f)
    (
        df.write
        .parquet(out_path)
    )
    
    print(f"✅ Done. Wrote to: {out_path}")

