"""
Incremental vs Full Load - ETL Learning Hub
==========================================
This module demonstrates incremental and full load strategies using PySpark.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, max as spark_max, lit, current_timestamp


def demonstrate_full_load(spark):
    """Demonstrate full load strategy."""
    print("\n" + "=" * 60)
    print("FULL LOAD STRATEGY")
    print("=" * 60)
    
    # Simulate source data
    source_df = spark.createDataFrame([
        (1, "Alice", 50000, "2024-01-01"),
        (2, "Bob", 60000, "2024-01-02"),
        (3, "Charlie", 75000, "2024-01-03")
    ], ["id", "name", "salary", "updated_at"])
    
    print("\nSource data (all records):")
    source_df.show()
    
    print("Full Load Process:")
    print("1. Extract ALL records from source")
    print("2. Transform data")
    print("3. OVERWRITE target (replace all)")
    print(f"\nRecords processed: {source_df.count()}")


def demonstrate_incremental_load(spark):
    """Demonstrate incremental load strategy."""
    print("\n" + "=" * 60)
    print("INCREMENTAL LOAD STRATEGY")
    print("=" * 60)
    
    # Simulate existing target data
    existing_df = spark.createDataFrame([
        (1, "Alice", 50000, "2024-01-01"),
        (2, "Bob", 60000, "2024-01-02")
    ], ["id", "name", "salary", "updated_at"])
    
    # Simulate new source data
    source_df = spark.createDataFrame([
        (1, "Alice", 50000, "2024-01-01"),
        (2, "Bob", 60000, "2024-01-02"),
        (3, "Charlie", 75000, "2024-01-03"),
        (4, "Diana", 55000, "2024-01-04")
    ], ["id", "name", "salary", "updated_at"])
    
    # Watermark (last processed timestamp)
    last_watermark = "2024-01-02"
    
    print(f"\nLast watermark: {last_watermark}")
    print("\nExisting target data:")
    existing_df.show()
    
    # Extract only new records
    new_records = source_df.filter(col("updated_at") > last_watermark)
    
    print("New records to process:")
    new_records.show()
    
    print("Incremental Load Process:")
    print("1. Get last processed watermark")
    print("2. Extract only records AFTER watermark")
    print("3. Transform new data")
    print("4. APPEND to target")
    print("5. Update watermark")
    print(f"\nRecords processed: {new_records.count()} (vs {source_df.count()} total)")


def main():
    spark = SparkSession.builder.appName("IncrementalVsFull").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    
    demonstrate_full_load(spark)
    demonstrate_incremental_load(spark)
    
    spark.stop()
    print("\n" + "=" * 60)
    print("ALL DEMONSTRATIONS COMPLETED!")
    print("=" * 60)


if __name__ == "__main__":
    main()
