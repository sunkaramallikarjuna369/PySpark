"""
Data Marts - DW Learning Hub
============================
This module demonstrates data mart concepts using PySpark.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as spark_sum, count, avg


def demonstrate_data_mart_creation(spark):
    """Demonstrate creating a data mart."""
    print("\n" + "=" * 60)
    print("DATA MART CREATION")
    print("=" * 60)
    
    # Simulate warehouse fact table
    fact_sales = spark.createDataFrame([
        (20240115, 1, 1, "Electronics", "Enterprise", 2, 999.99, 1999.98),
        (20240115, 2, 2, "Clothing", "SMB", 5, 29.99, 149.95),
        (20240116, 1, 1, "Electronics", "Enterprise", 1, 999.99, 999.99),
        (20240116, 3, 2, "Home", "Consumer", 3, 49.99, 149.97)
    ], ["date_key", "product_key", "customer_key", "category", "segment", "quantity", "unit_price", "total_amount"])
    
    print("\nWarehouse Fact Table:")
    fact_sales.show()
    
    # Create Sales Data Mart (denormalized, pre-aggregated)
    sales_mart = fact_sales.groupBy("date_key", "category", "segment").agg(
        spark_sum("quantity").alias("total_units"),
        spark_sum("total_amount").alias("total_sales"),
        count("*").alias("transaction_count")
    )
    
    print("Sales Data Mart (aggregated):")
    sales_mart.show()
    
    # Create Category Summary for quick reporting
    category_summary = fact_sales.groupBy("category").agg(
        spark_sum("total_amount").alias("total_sales"),
        avg("unit_price").alias("avg_price")
    )
    
    print("Category Summary (pre-computed):")
    category_summary.show()


def demonstrate_data_mart_types(spark):
    """Demonstrate data mart types."""
    print("\n" + "=" * 60)
    print("DATA MART TYPES")
    print("=" * 60)
    
    print("\n1. DEPENDENT DATA MART")
    print("   - Created FROM enterprise data warehouse")
    print("   - Ensures consistency")
    print("   - Recommended approach")
    
    print("\n2. INDEPENDENT DATA MART")
    print("   - Created directly from sources")
    print("   - Faster to build")
    print("   - Risk of data silos")
    
    print("\n3. HYBRID DATA MART")
    print("   - Combines both approaches")
    print("   - Flexibility with some consistency")


def main():
    spark = SparkSession.builder.appName("DataMarts").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    
    demonstrate_data_mart_creation(spark)
    demonstrate_data_mart_types(spark)
    
    spark.stop()
    print("\n" + "=" * 60)
    print("ALL DEMONSTRATIONS COMPLETED!")
    print("=" * 60)


if __name__ == "__main__":
    main()
