"""
OLAP vs OLTP - DW Learning Hub
==============================
This module demonstrates OLAP vs OLTP concepts using PySpark.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as spark_sum, count, avg


def demonstrate_oltp_patterns(spark):
    """Demonstrate OLTP query patterns."""
    print("\n" + "=" * 60)
    print("OLTP PATTERNS")
    print("=" * 60)
    
    print("\nOLTP Characteristics:")
    print("- Simple, fast queries")
    print("- Row-level operations")
    print("- INSERT, UPDATE, DELETE heavy")
    print("- Normalized schema (3NF)")
    print("- Current data focus")
    
    # Simulate OLTP table
    orders = spark.createDataFrame([
        (1, 101, "2024-01-15", "completed", 150.00),
        (2, 102, "2024-01-15", "pending", 75.00),
        (3, 101, "2024-01-16", "completed", 200.00)
    ], ["order_id", "customer_id", "order_date", "status", "amount"])
    
    print("\nOLTP Query: Get customer's recent orders")
    orders.filter(col("customer_id") == 101).orderBy(col("order_date").desc()).show()


def demonstrate_olap_patterns(spark):
    """Demonstrate OLAP query patterns."""
    print("\n" + "=" * 60)
    print("OLAP PATTERNS")
    print("=" * 60)
    
    print("\nOLAP Characteristics:")
    print("- Complex analytical queries")
    print("- Aggregations and groupings")
    print("- SELECT heavy (read-only)")
    print("- Denormalized schema (Star/Snowflake)")
    print("- Historical data focus")
    
    # Simulate fact table
    fact_sales = spark.createDataFrame([
        (20240115, 1, 1, 100.00),
        (20240115, 2, 1, 150.00),
        (20240115, 1, 2, 200.00),
        (20240116, 1, 1, 120.00),
        (20240116, 2, 2, 180.00)
    ], ["date_key", "product_key", "region_key", "sales_amount"])
    
    fact_sales.createOrReplaceTempView("fact_sales")
    
    print("\nOLAP Query: Sales by Product and Region")
    spark.sql("""
        SELECT 
            product_key,
            region_key,
            SUM(sales_amount) as total_sales,
            COUNT(*) as transaction_count,
            AVG(sales_amount) as avg_sale
        FROM fact_sales
        GROUP BY product_key, region_key
        ORDER BY total_sales DESC
    """).show()
    
    print("OLAP Query: Roll-up by Date")
    spark.sql("""
        SELECT 
            date_key,
            SUM(sales_amount) as daily_sales
        FROM fact_sales
        GROUP BY date_key
        ORDER BY date_key
    """).show()


def main():
    spark = SparkSession.builder.appName("OLAP_OLTP").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    
    demonstrate_oltp_patterns(spark)
    demonstrate_olap_patterns(spark)
    
    spark.stop()
    print("\n" + "=" * 60)
    print("ALL DEMONSTRATIONS COMPLETED!")
    print("=" * 60)


if __name__ == "__main__":
    main()
