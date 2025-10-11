from pyspark.sql.types import *

services_schema = StructType([
    StructField("CustomerID", StringType()),
    StructField("Count", IntegerType()),
    StructField("Quarter", StringType()),
    StructField("Referred a Friend", StringType()),
    StructField("Number of Referrals", IntegerType()),
    StructField("Tenure in Months", IntegerType()),
    StructField("Offer", StringType()),
    StructField("Phone Service", StringType()),
    StructField("Avg Monthly Long Distance Charges", DoubleType()),
    StructField("Multiple Lines", StringType()),
    StructField("Internet Service", StringType()),
    StructField("Avg Monthly GB Download", DoubleType()),
    StructField("Online Security", StringType()),
    StructField("Online Backup", StringType()),
    StructField("Device Protection Plan", StringType()),
    StructField("Premium Tech Support", StringType()),
    StructField("Streaming TV", StringType()),
    StructField("Streaming Movies", StringType()),
    StructField("Streaming Music", StringType()),
    StructField("Unlimited Data", StringType()),
    StructField("Contract", StringType()),
    StructField("Paperless Billing", StringType()),
    StructField("Payment Method", StringType()),
    StructField("Monthly Charge", DoubleType()),
    StructField("Total Charges", DoubleType()),
    StructField("Total Refunds", DoubleType()),
    StructField("Total Extra Data Charges", DoubleType()),
    StructField("Total Long Distance Charges", DoubleType())
])

status_schema = StructType([
    StructField("CustomerID", StringType()),
    StructField("Count", IntegerType()),
    StructField("Quarter", StringType()),
    StructField("Satisfaction Score", IntegerType()),
    StructField("Satisfaction Score Label", StringType()),
    StructField("Customer Status", StringType()),
    StructField("Churn Label", StringType()),
    StructField("Churn Value", IntegerType()),
    StructField("Churn Score", IntegerType()),
    StructField("Churn Score Category", StringType()),
    StructField("CLTV", DoubleType()),
    StructField("CLTV Category", StringType()),
    StructField("Churn Category", StringType()),
    StructField("Churn Reason", StringType())
])

demographics_schema = StructType([
    StructField("CustomerID", StringType()),
    StructField("Count", IntegerType()),
    StructField("Gender", StringType()),
    StructField("Age", IntegerType()),
    StructField("Senior Citizen", StringType()),
    StructField("Married", StringType()),
    StructField("Dependents", StringType()),
    StructField("Number of Dependents", IntegerType())
])

location_schema = StructType([
    StructField("CustomerID", StringType()),
    StructField("Count", IntegerType()),
    StructField("Country", StringType()),
    StructField("State", StringType()),
    StructField("City", StringType()),
    StructField("Zip Code", StringType()),
    StructField("Lat Long", StringType()),
    StructField("Latitude", DoubleType()),
    StructField("Longitude", DoubleType())
])

population_schema = StructType([
    StructField("ID", StringType()),
    StructField("Zip Code", StringType()),
    StructField("Population", IntegerType())
])
