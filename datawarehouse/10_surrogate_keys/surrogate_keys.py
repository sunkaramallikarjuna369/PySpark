"""
Surrogate Keys - DW Learning Hub
================================
This module demonstrates surrogate key concepts using PySpark.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, row_number, coalesce, lit, broadcast
from pyspark.sql.window import Window


def demonstrate_surrogate_keys(spark):
    """Demonstrate surrogate key generation."""
    print("\n" + "=" * 60)
    print("SURROGATE KEY GENERATION")
    print("=" * 60)
    
    # Source data with natural keys
    source_df = spark.createDataFrame([
        ("C001", "Alice", "Enterprise"),
        ("C002", "Bob", "SMB"),
        ("C003", "Charlie", "Consumer")
    ], ["customer_id", "name", "segment"])
    
    print("\nSource data with natural keys:")
    source_df.show()
    
    # Generate surrogate keys
    window = Window.orderBy("customer_id")
    dim_customer = source_df.withColumn("customer_key", row_number().over(window))
    
    # Reorder columns
    dim_customer = dim_customer.select("customer_key", "customer_id", "name", "segment")
    
    print("Dimension with surrogate keys:")
    dim_customer.show()
    
    print("Key points:")
    print("- customer_key: Surrogate (system-generated)")
    print("- customer_id: Natural (from source)")


def demonstrate_key_lookup(spark):
    """Demonstrate surrogate key lookup for fact loading."""
    print("\n" + "=" * 60)
    print("SURROGATE KEY LOOKUP")
    print("=" * 60)
    
    # Dimension table
    dim_customer = spark.createDataFrame([
        (1, "C001", "Alice"),
        (2, "C002", "Bob"),
        (-1, "UNKNOWN", "Unknown")  # Unknown member
    ], ["customer_key", "customer_id", "name"])
    
    # Staging fact with natural keys
    fact_staging = spark.createDataFrame([
        ("2024-01-15", "C001", 100.00),
        ("2024-01-15", "C002", 150.00),
        ("2024-01-15", "C999", 75.00)  # Unknown customer
    ], ["date", "customer_id", "amount"])
    
    print("\nStaging fact (natural keys):")
    fact_staging.show()
    
    # Lookup surrogate keys
    fact_with_keys = fact_staging.join(
        broadcast(dim_customer.select("customer_id", "customer_key")),
        on="customer_id",
        how="left"
    ).withColumn(
        "customer_key",
        coalesce(col("customer_key"), lit(-1))
    ).select("date", "customer_key", "amount")
    
    print("Fact with surrogate keys:")
    fact_with_keys.show()
    
    print("Note: C999 mapped to -1 (unknown)")


def main():
    spark = SparkSession.builder.appName("SurrogateKeys").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    
    demonstrate_surrogate_keys(spark)
    demonstrate_key_lookup(spark)
    
    spark.stop()
    print("\n" + "=" * 60)
    print("ALL DEMONSTRATIONS COMPLETED!")
    print("=" * 60)


if __name__ == "__main__":
    main()
