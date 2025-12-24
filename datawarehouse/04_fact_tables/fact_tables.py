"""
Fact Tables - DW Learning Hub
=============================
This module demonstrates fact table concepts using PySpark.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as spark_sum, count, avg


def demonstrate_fact_types(spark):
    """Demonstrate different fact table types."""
    print("\n" + "=" * 60)
    print("FACT TABLE TYPES")
    print("=" * 60)
    
    # 1. Transaction Fact Table
    print("\n1. TRANSACTION FACT TABLE")
    print("   - Records individual transactions")
    print("   - Finest grain, most flexible")
    transaction_fact = spark.createDataFrame([
        (1, 20240115, 1, 1, 2, 999.99, 1999.98),
        (2, 20240115, 2, 2, 5, 29.99, 149.95),
        (3, 20240116, 1, 3, 1, 999.99, 999.99)
    ], ["transaction_id", "date_key", "product_key", "customer_key", "quantity", "unit_price", "total_amount"])
    transaction_fact.show()
    
    # 2. Periodic Snapshot Fact Table
    print("2. PERIODIC SNAPSHOT FACT TABLE")
    print("   - Captures state at regular intervals")
    snapshot_fact = spark.createDataFrame([
        (20240115, 1, 100, 50),
        (20240115, 2, 200, 30),
        (20240116, 1, 98, 50),
        (20240116, 2, 195, 30)
    ], ["snapshot_date_key", "product_key", "quantity_on_hand", "reorder_point"])
    snapshot_fact.show()
    
    # 3. Factless Fact Table
    print("3. FACTLESS FACT TABLE")
    print("   - Records events without measures")
    factless_fact = spark.createDataFrame([
        (20240115, 1, 101),
        (20240115, 2, 101),
        (20240115, 1, 102)
    ], ["date_key", "student_key", "class_key"])
    factless_fact.show()


def demonstrate_measures(spark):
    """Demonstrate measure types."""
    print("\n" + "=" * 60)
    print("MEASURE TYPES")
    print("=" * 60)
    
    fact_df = spark.createDataFrame([
        (1, 20240115, 1, 100, 10.00, 1000.00, 0.25),
        (2, 20240115, 2, 50, 20.00, 1000.00, 0.30),
        (3, 20240116, 1, 75, 10.00, 750.00, 0.25)
    ], ["id", "date_key", "product_key", "quantity", "unit_price", "total_amount", "profit_margin"])
    
    print("\nFact data:")
    fact_df.show()
    
    print("ADDITIVE: SUM(total_amount) - works across all dimensions")
    fact_df.groupBy("product_key").agg(spark_sum("total_amount").alias("total_sales")).show()
    
    print("SEMI-ADDITIVE: AVG over time for balances/inventory")
    print("NON-ADDITIVE: AVG(profit_margin) - cannot sum percentages")
    fact_df.groupBy("product_key").agg(avg("profit_margin").alias("avg_margin")).show()


def main():
    spark = SparkSession.builder.appName("FactTables").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    
    demonstrate_fact_types(spark)
    demonstrate_measures(spark)
    
    spark.stop()
    print("\n" + "=" * 60)
    print("ALL DEMONSTRATIONS COMPLETED!")
    print("=" * 60)


if __name__ == "__main__":
    main()
