from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, cast

def main():
    # Initialize Spark
    spark = SparkSession.builder \
        .appName("OlistRawToStaging") \
        .getOrCreate()

    # CONFIGURATION - Replace with your actual bucket name from Terraform
    STAGING_BUCKET = "olist-staging-zone-arjun-cs-032026"
    
    # 1. Read the JSON files we uploaded in Phase 2
    # We use a wildcard (*) to grab all folders under 'raw/orders'
    raw_df = spark.read.json(f"s3a://olist-raw-zone-arjun-cs-032026/raw/orders/*/*.json")

    print("Raw Data Schema:")
    raw_df.printSchema()

    # 2. Transformation Logic (The 'Engineering' part)
    # We convert strings to Timestamps and verify IDs
    clean_df = raw_df.withColumn("order_purchase_timestamp", to_timestamp(col("order_purchase_timestamp"))) \
                     .withColumn("order_approved_at", to_timestamp(col("order_approved_at"))) \
                     .withColumn("order_delivered_carrier_date", to_timestamp(col("order_delivered_carrier_date"))) \
                     .withColumn("order_delivered_customer_date", to_timestamp(col("order_delivered_customer_date"))) \
                     .select(
                         "order_id", 
                         "customer_id", 
                         "order_status", 
                         "order_purchase_timestamp", 
                         "order_delivered_customer_date"
                     )

    # 3. Write to Staging as Parquet
    # 'overwrite' ensures we don't double the data if we run this twice (Idempotency)
    output_path = f"s3a://{STAGING_BUCKET}/staging/orders/"
    
    print(f"Writing cleaned data to {output_path}...")
    
    clean_df.write.mode("overwrite").parquet(output_path)

    print("Process Complete!")

if __name__ == "__main__":
    main()