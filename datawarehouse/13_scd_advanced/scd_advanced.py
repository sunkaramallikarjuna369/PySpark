"""
SCD Advanced Techniques - DW Learning Hub
=========================================
This module demonstrates advanced SCD techniques using PySpark.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, current_timestamp, hash as spark_hash, abs as spark_abs


def demonstrate_hash_change_detection(spark):
    """Demonstrate hash-based change detection."""
    print("\n" + "=" * 60)
    print("HASH-BASED CHANGE DETECTION")
    print("=" * 60)
    
    # Source data (new/updated)
    source_df = spark.createDataFrame([
        ("C001", "Alice", "Los Angeles", "Enterprise"),  # Changed city
        ("C002", "Bob", "Chicago", "SMB"),  # No change
        ("C003", "Charlie", "Boston", "Consumer")  # New record
    ], ["customer_id", "name", "city", "segment"])
    
    # Target data (existing)
    target_df = spark.createDataFrame([
        (1, "C001", "Alice", "New York", "Enterprise", True),
        (2, "C002", "Bob", "Chicago", "SMB", True)
    ], ["customer_key", "customer_id", "name", "city", "segment", "is_current"])
    
    tracked_columns = ["name", "city", "segment"]
    
    # Add hash to source
    source_with_hash = source_df.withColumn(
        "row_hash",
        spark_abs(spark_hash(*[col(c) for c in tracked_columns]))
    )
    
    # Add hash to target
    target_with_hash = target_df.filter("is_current = true").withColumn(
        "row_hash",
        spark_abs(spark_hash(*[col(c) for c in tracked_columns]))
    )
    
    print("\nSource with hash:")
    source_with_hash.show()
    
    print("Target with hash:")
    target_with_hash.show()
    
    # Detect changes by comparing hashes
    changes = source_with_hash.alias("s").join(
        target_with_hash.alias("t"),
        on="customer_id",
        how="left"
    ).filter(
        (col("t.row_hash").isNull()) |
        (col("s.row_hash") != col("t.row_hash"))
    ).select("s.*")
    
    print("Detected changes (new or modified):")
    changes.show()


def demonstrate_scd_optimization(spark):
    """Demonstrate SCD optimization techniques."""
    print("\n" + "=" * 60)
    print("SCD OPTIMIZATION TECHNIQUES")
    print("=" * 60)
    
    print("\n1. HASH-BASED CHANGE DETECTION")
    print("   - Compare hash instead of all columns")
    print("   - Much faster for wide tables")
    
    print("\n2. PARTITION PRUNING")
    print("   - Partition by effective_date")
    print("   - Query only relevant partitions")
    
    print("\n3. Z-ORDERING (Delta Lake)")
    print("   - OPTIMIZE table ZORDER BY (natural_key, is_current)")
    print("   - Faster lookups on common columns")
    
    print("\n4. INCREMENTAL PROCESSING")
    print("   - Process only new/changed records")
    print("   - Use watermarks to track progress")
    
    print("\n5. MINI-DIMENSIONS")
    print("   - Split volatile attributes")
    print("   - Reduce SCD overhead")


def main():
    spark = SparkSession.builder.appName("SCDAdvanced").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    
    demonstrate_hash_change_detection(spark)
    demonstrate_scd_optimization(spark)
    
    spark.stop()
    print("\n" + "=" * 60)
    print("ALL DEMONSTRATIONS COMPLETED!")
    print("=" * 60)


if __name__ == "__main__":
    main()
