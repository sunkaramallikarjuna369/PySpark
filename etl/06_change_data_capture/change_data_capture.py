"""
Change Data Capture (CDC) - ETL Learning Hub
============================================
This module demonstrates CDC concepts using PySpark.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, when, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType


def demonstrate_cdc_concepts(spark):
    """Demonstrate CDC concepts."""
    print("\n" + "=" * 60)
    print("CHANGE DATA CAPTURE (CDC) CONCEPTS")
    print("=" * 60)
    
    # Simulate CDC events
    cdc_events = spark.createDataFrame([
        ("I", 1, "Alice", 50000, None, None),
        ("I", 2, "Bob", 60000, None, None),
        ("U", 1, "Alice", 55000, "Alice", 50000),
        ("D", 2, None, None, "Bob", 60000),
        ("I", 3, "Charlie", 75000, None, None)
    ], ["operation", "id", "name_after", "salary_after", "name_before", "salary_before"])
    
    print("\nCDC Events:")
    print("I = Insert, U = Update, D = Delete")
    cdc_events.show()
    
    print("\nCDC Methods:")
    print("1. Timestamp-based: Track updated_at column")
    print("2. Trigger-based: Database triggers to audit table")
    print("3. Log-based: Read transaction logs (Debezium)")
    print("4. Diff-based: Compare snapshots")


def demonstrate_cdc_processing(spark):
    """Demonstrate CDC processing."""
    print("\n" + "=" * 60)
    print("CDC PROCESSING")
    print("=" * 60)
    
    # Current state of target
    target_df = spark.createDataFrame([
        (1, "Alice", 50000),
        (2, "Bob", 60000)
    ], ["id", "name", "salary"])
    
    print("\nCurrent target state:")
    target_df.show()
    
    # CDC events to apply
    cdc_events = spark.createDataFrame([
        ("U", 1, "Alice", 55000),  # Update Alice's salary
        ("D", 2, "Bob", 60000),    # Delete Bob
        ("I", 3, "Charlie", 75000) # Insert Charlie
    ], ["operation", "id", "name", "salary"])
    
    print("CDC events to apply:")
    cdc_events.show()
    
    # Process CDC events
    inserts = cdc_events.filter(col("operation") == "I").drop("operation")
    updates = cdc_events.filter(col("operation") == "U").drop("operation")
    deletes = cdc_events.filter(col("operation") == "D").select("id")
    
    # Apply changes (simplified)
    # In production, use Delta Lake MERGE
    remaining = target_df.join(deletes, "id", "left_anti")
    updated = remaining.join(updates.select("id", "salary"), "id", "left") \
        .withColumn("salary", when(col("salary").isNotNull(), col("salary")).otherwise(col("salary")))
    
    final_df = remaining.union(inserts)
    
    print("After applying CDC events:")
    final_df.show()


def main():
    spark = SparkSession.builder.appName("CDC").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    
    demonstrate_cdc_concepts(spark)
    demonstrate_cdc_processing(spark)
    
    spark.stop()
    print("\n" + "=" * 60)
    print("ALL DEMONSTRATIONS COMPLETED!")
    print("=" * 60)


if __name__ == "__main__":
    main()
