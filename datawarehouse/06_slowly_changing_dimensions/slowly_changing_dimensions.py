"""
Slowly Changing Dimensions (SCD) - DW Learning Hub
=================================================
This module demonstrates SCD techniques using PySpark.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, current_date, when, max as spark_max


def demonstrate_scd_type1(spark):
    """Demonstrate SCD Type 1 - Overwrite."""
    print("\n" + "=" * 60)
    print("SCD TYPE 1: OVERWRITE")
    print("=" * 60)
    
    # Current dimension state
    dim_customer = spark.createDataFrame([
        (1, "C001", "Alice", "New York", "Enterprise"),
        (2, "C002", "Bob", "Chicago", "SMB")
    ], ["customer_key", "customer_id", "name", "city", "segment"])
    
    print("\nBefore update:")
    dim_customer.show()
    
    # Incoming change: Alice moved to Los Angeles
    update_data = spark.createDataFrame([
        ("C001", "Alice", "Los Angeles", "Enterprise")
    ], ["customer_id", "name", "city", "segment"])
    
    print("Incoming change (Alice moved):")
    update_data.show()
    
    # Type 1: Simply overwrite
    # In production, use Delta Lake MERGE
    print("After Type 1 update (history lost):")
    print("Alice's city changed from 'New York' to 'Los Angeles'")
    print("Old value 'New York' is NOT preserved")


def demonstrate_scd_type2(spark):
    """Demonstrate SCD Type 2 - Add New Row."""
    print("\n" + "=" * 60)
    print("SCD TYPE 2: ADD NEW ROW (FULL HISTORY)")
    print("=" * 60)
    
    # Current dimension state with SCD2 columns
    dim_customer = spark.createDataFrame([
        (1, "C001", "Alice", "New York", "2023-01-01", "9999-12-31", True),
        (2, "C002", "Bob", "Chicago", "2023-01-01", "9999-12-31", True)
    ], ["customer_key", "customer_id", "name", "city", "effective_date", "expiration_date", "is_current"])
    
    print("\nBefore update:")
    dim_customer.show()
    
    # After Type 2 update: Alice moved to Los Angeles
    dim_customer_after = spark.createDataFrame([
        (1, "C001", "Alice", "New York", "2023-01-01", "2024-01-15", False),
        (2, "C002", "Bob", "Chicago", "2023-01-01", "9999-12-31", True),
        (3, "C001", "Alice", "Los Angeles", "2024-01-15", "9999-12-31", True)
    ], ["customer_key", "customer_id", "name", "city", "effective_date", "expiration_date", "is_current"])
    
    print("After Type 2 update (history preserved):")
    dim_customer_after.show()
    
    print("Key points:")
    print("- Old record expired (is_current=False)")
    print("- New record added with new surrogate key")
    print("- Full history preserved for analysis")


def demonstrate_scd_type3(spark):
    """Demonstrate SCD Type 3 - Add Column."""
    print("\n" + "=" * 60)
    print("SCD TYPE 3: ADD COLUMN (PREVIOUS VALUE)")
    print("=" * 60)
    
    # Type 3 schema with previous value column
    dim_customer = spark.createDataFrame([
        (1, "C001", "Alice", "Los Angeles", "New York", "2024-01-15"),
        (2, "C002", "Bob", "Chicago", None, None)
    ], ["customer_key", "customer_id", "name", "current_city", "previous_city", "city_change_date"])
    
    print("\nType 3 structure:")
    dim_customer.show()
    
    print("Key points:")
    print("- current_city: Current value")
    print("- previous_city: Previous value (limited history)")
    print("- Simpler than Type 2, but only tracks one change")


def main():
    spark = SparkSession.builder.appName("SCD").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    
    demonstrate_scd_type1(spark)
    demonstrate_scd_type2(spark)
    demonstrate_scd_type3(spark)
    
    spark.stop()
    print("\n" + "=" * 60)
    print("ALL DEMONSTRATIONS COMPLETED!")
    print("=" * 60)


if __name__ == "__main__":
    main()
