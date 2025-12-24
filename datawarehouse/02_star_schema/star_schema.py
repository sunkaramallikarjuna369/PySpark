"""
Star Schema - DW Learning Hub
=============================
This module demonstrates star schema design using PySpark.
Includes examples using both inline data and CSV files from data/ directory.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as spark_sum, count, concat, lit
import os


def load_from_csv(spark, data_path="../../data"):
    """Load star schema tables from CSV files."""
    print("\n" + "=" * 60)
    print("LOADING DATA FROM CSV FILES")
    print("=" * 60)
    
    # Load dimension tables from CSV
    dim_date = spark.read.csv(f"{data_path}/dates.csv", header=True, inferSchema=True)
    dim_product = spark.read.csv(f"{data_path}/products.csv", header=True, inferSchema=True)
    dim_customer = spark.read.csv(f"{data_path}/customers.csv", header=True, inferSchema=True)
    
    # Load fact table from CSV
    fact_sales = spark.read.csv(f"{data_path}/sales.csv", header=True, inferSchema=True)
    
    print(f"\nLoaded {dim_date.count()} date records")
    print(f"Loaded {dim_product.count()} product records")
    print(f"Loaded {dim_customer.count()} customer records")
    print(f"Loaded {fact_sales.count()} sales records")
    
    return dim_date, dim_product, dim_customer, fact_sales


def create_star_schema(spark):
    """Create star schema tables with inline data."""
    print("\n" + "=" * 60)
    print("STAR SCHEMA DESIGN")
    print("=" * 60)
    
    # Dimension: Date
    dim_date = spark.createDataFrame([
        (20240115, "2024-01-15", "Monday", 1, "January", 2024),
        (20240116, "2024-01-16", "Tuesday", 1, "January", 2024),
        (20240117, "2024-01-17", "Wednesday", 1, "January", 2024)
    ], ["date_key", "full_date", "day_name", "quarter", "month_name", "year"])
    
    # Dimension: Product
    dim_product = spark.createDataFrame([
        (1, "P001", "Laptop", "Electronics", 999.99),
        (2, "P002", "Mouse", "Electronics", 29.99),
        (3, "P003", "T-Shirt", "Clothing", 19.99)
    ], ["product_key", "product_id", "product_name", "category", "unit_price"])
    
    # Dimension: Customer
    dim_customer = spark.createDataFrame([
        (1, "C001", "Alice Smith", "New York", "Enterprise"),
        (2, "C002", "Bob Johnson", "Los Angeles", "SMB"),
        (3, "C003", "Charlie Brown", "Chicago", "Consumer")
    ], ["customer_key", "customer_id", "customer_name", "city", "segment"])
    
    # Fact: Sales
    fact_sales = spark.createDataFrame([
        (1, 20240115, 1, 1, 2, 999.99, 1999.98),
        (2, 20240115, 2, 2, 5, 29.99, 149.95),
        (3, 20240116, 3, 1, 3, 19.99, 59.97),
        (4, 20240116, 1, 3, 1, 999.99, 999.99),
        (5, 20240117, 2, 2, 10, 29.99, 299.90)
    ], ["sale_key", "date_key", "product_key", "customer_key", "quantity", "unit_price", "total_amount"])
    
    print("\nDimension: Date")
    dim_date.show()
    
    print("Dimension: Product")
    dim_product.show()
    
    print("Dimension: Customer")
    dim_customer.show()
    
    print("Fact: Sales")
    fact_sales.show()
    
    return dim_date, dim_product, dim_customer, fact_sales


def query_star_schema(spark, dim_date, dim_product, dim_customer, fact_sales):
    """Query star schema."""
    print("\n" + "=" * 60)
    print("STAR SCHEMA QUERIES")
    print("=" * 60)
    
    # Register tables
    dim_date.createOrReplaceTempView("dim_date")
    dim_product.createOrReplaceTempView("dim_product")
    dim_customer.createOrReplaceTempView("dim_customer")
    fact_sales.createOrReplaceTempView("fact_sales")
    
    # Sales by Category
    print("\nSales by Product Category:")
    spark.sql("""
        SELECT p.category, SUM(f.total_amount) as total_sales
        FROM fact_sales f
        JOIN dim_product p ON f.product_key = p.product_key
        GROUP BY p.category
        ORDER BY total_sales DESC
    """).show()
    
    # Sales by Customer Segment
    print("Sales by Customer Segment:")
    spark.sql("""
        SELECT c.segment, COUNT(*) as transactions, SUM(f.total_amount) as total_sales
        FROM fact_sales f
        JOIN dim_customer c ON f.customer_key = c.customer_key
        GROUP BY c.segment
    """).show()


def query_csv_data(spark, data_path="../../data"):
    """Query star schema using CSV data files."""
    print("\n" + "=" * 60)
    print("QUERYING CSV DATA")
    print("=" * 60)
    
    dim_date, dim_product, dim_customer, fact_sales = load_from_csv(spark, data_path)
    
    # Register tables
    dim_date.createOrReplaceTempView("dim_date")
    dim_product.createOrReplaceTempView("dim_product")
    dim_customer.createOrReplaceTempView("dim_customer")
    fact_sales.createOrReplaceTempView("fact_sales")
    
    # Sales by Category from CSV
    print("\nSales by Product Category (from CSV):")
    spark.sql("""
        SELECT p.category, 
               COUNT(*) as transactions,
               SUM(s.total_amount) as total_sales
        FROM fact_sales s
        JOIN dim_product p ON s.product_id = p.product_id
        GROUP BY p.category
        ORDER BY total_sales DESC
    """).show()
    
    # Sales by Customer Segment from CSV
    print("Sales by Customer Segment (from CSV):")
    spark.sql("""
        SELECT c.segment, 
               COUNT(*) as transactions,
               SUM(s.total_amount) as total_sales
        FROM fact_sales s
        JOIN dim_customer c ON s.customer_id = c.customer_id
        GROUP BY c.segment
        ORDER BY total_sales DESC
    """).show()


def main():
    spark = SparkSession.builder.appName("StarSchema").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    
    # Demo with inline data
    dim_date, dim_product, dim_customer, fact_sales = create_star_schema(spark)
    query_star_schema(spark, dim_date, dim_product, dim_customer, fact_sales)
    
    # Demo with CSV data (uncomment to use)
    # query_csv_data(spark, "../../data")
    
    spark.stop()
    print("\n" + "=" * 60)
    print("ALL DEMONSTRATIONS COMPLETED!")
    print("=" * 60)
    print("\nTo use CSV data, uncomment query_csv_data() in main()")
    print("CSV files are located in the data/ directory")


if __name__ == "__main__":
    main()
