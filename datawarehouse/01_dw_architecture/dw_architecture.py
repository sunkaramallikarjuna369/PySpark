"""
Data Warehouse Architecture - DW Learning Hub
=============================================
This module demonstrates data warehouse architecture concepts using PySpark.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, current_timestamp


def demonstrate_medallion_architecture(spark):
    """Demonstrate medallion architecture layers."""
    print("\n" + "=" * 60)
    print("MEDALLION ARCHITECTURE")
    print("=" * 60)
    
    # Simulate Bronze layer (raw data)
    bronze_data = [
        ("TXN001", "2024-01-15 10:30:00", "Electronics", 299.99, "completed"),
        ("TXN002", "2024-01-15 11:45:00", "Clothing", 89.99, "completed"),
        ("TXN001", "2024-01-15 10:30:00", "Electronics", 299.99, "completed"),  # Duplicate
        ("TXN003", "2024-01-15 14:20:00", "Electronics", -50.00, "refund")
    ]
    bronze_df = spark.createDataFrame(bronze_data, 
        ["transaction_id", "timestamp", "category", "amount", "status"])
    
    print("\nBRONZE LAYER (Raw):")
    bronze_df.show()
    
    # Silver layer (cleansed)
    silver_df = bronze_df \
        .dropDuplicates(["transaction_id"]) \
        .filter(col("amount") > 0) \
        .filter(col("status") == "completed")
    
    print("SILVER LAYER (Cleansed):")
    silver_df.show()
    
    # Gold layer (aggregated)
    silver_df.createOrReplaceTempView("silver_sales")
    gold_df = spark.sql("""
        SELECT 
            category,
            COUNT(*) as transaction_count,
            SUM(amount) as total_amount
        FROM silver_sales
        GROUP BY category
    """)
    
    print("GOLD LAYER (Business-ready):")
    gold_df.show()


def demonstrate_dw_concepts(spark):
    """Demonstrate core DW concepts."""
    print("\n" + "=" * 60)
    print("DATA WAREHOUSE CHARACTERISTICS")
    print("=" * 60)
    
    print("\n1. SUBJECT-ORIENTED")
    print("   - Organized around business subjects (customers, products)")
    
    print("\n2. INTEGRATED")
    print("   - Data from multiple sources combined consistently")
    
    print("\n3. TIME-VARIANT")
    print("   - Historical data maintained for trend analysis")
    
    print("\n4. NON-VOLATILE")
    print("   - Data loaded and accessed, not updated in place")


def main():
    spark = SparkSession.builder.appName("DWArchitecture").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    
    demonstrate_dw_concepts(spark)
    demonstrate_medallion_architecture(spark)
    
    spark.stop()
    print("\n" + "=" * 60)
    print("ALL DEMONSTRATIONS COMPLETED!")
    print("=" * 60)


if __name__ == "__main__":
    main()
