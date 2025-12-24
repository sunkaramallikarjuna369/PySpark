"""
Data Warehouse Best Practices - DW Learning Hub
===============================================
This module demonstrates DW best practices using PySpark.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, sum as spark_sum


def demonstrate_design_practices(spark):
    """Demonstrate design best practices."""
    print("\n" + "=" * 60)
    print("DESIGN BEST PRACTICES")
    print("=" * 60)
    
    print("\n1. DEFINE CLEAR GRAIN")
    print("   - One row = one transaction")
    print("   - Document grain for every fact table")
    
    print("\n2. USE SURROGATE KEYS")
    print("   - Integer keys for performance")
    print("   - Handle unknown members (-1)")
    
    print("\n3. CONFORM DIMENSIONS")
    print("   - Share dimensions across facts")
    print("   - Enable drill-across queries")
    
    print("\n4. DESIGN FOR CHANGE")
    print("   - Plan for SCD from start")
    print("   - Use effective/expiration dates")


def demonstrate_performance_practices(spark):
    """Demonstrate performance best practices."""
    print("\n" + "=" * 60)
    print("PERFORMANCE BEST PRACTICES")
    print("=" * 60)
    
    # Sample fact table
    fact_sales = spark.createDataFrame([
        (20240115, 1, "Electronics", 1000.00),
        (20240115, 2, "Clothing", 500.00),
        (20240116, 1, "Electronics", 750.00)
    ], ["date_key", "product_key", "category", "amount"])
    
    print("\n1. PARTITIONING")
    print("   - Partition by date for time-based queries")
    
    print("\n2. PRE-AGGREGATION")
    # Create aggregation table
    agg_by_category = fact_sales.groupBy("category").agg(
        spark_sum("amount").alias("total_sales"),
        count("*").alias("transaction_count")
    )
    print("   Pre-aggregated summary:")
    agg_by_category.show()
    
    print("3. COLUMNAR STORAGE")
    print("   - Use Parquet, ORC, or Delta Lake")
    
    print("\n4. STATISTICS")
    print("   - Keep statistics updated for optimizer")


def demonstrate_governance_practices(spark):
    """Demonstrate governance best practices."""
    print("\n" + "=" * 60)
    print("GOVERNANCE BEST PRACTICES")
    print("=" * 60)
    
    print("\n1. DATA QUALITY")
    print("   - Define and enforce quality rules")
    
    print("\n2. DATA SECURITY")
    print("   - Role-based access control")
    print("   - Column-level security for PII")
    
    print("\n3. DATA LINEAGE")
    print("   - Track data from source to report")
    
    print("\n4. DOCUMENTATION")
    print("   - Data dictionary")
    print("   - Business rules")
    print("   - ETL documentation")


def main():
    spark = SparkSession.builder.appName("DWBestPractices").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    
    demonstrate_design_practices(spark)
    demonstrate_performance_practices(spark)
    demonstrate_governance_practices(spark)
    
    spark.stop()
    print("\n" + "=" * 60)
    print("ALL DEMONSTRATIONS COMPLETED!")
    print("=" * 60)


if __name__ == "__main__":
    main()
