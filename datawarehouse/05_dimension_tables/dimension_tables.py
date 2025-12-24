"""
Dimension Tables - DW Learning Hub
==================================
This module demonstrates dimension table concepts using PySpark.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, concat, lit


def create_dimensions(spark):
    """Create sample dimension tables."""
    print("\n" + "=" * 60)
    print("DIMENSION TABLES")
    print("=" * 60)
    
    # Date Dimension
    dim_date = spark.createDataFrame([
        (20240115, "2024-01-15", "Monday", 1, "January", 1, 2024, False),
        (20240116, "2024-01-16", "Tuesday", 1, "January", 1, 2024, False),
        (20240120, "2024-01-20", "Saturday", 1, "January", 1, 2024, True)
    ], ["date_key", "full_date", "day_name", "quarter", "month_name", "month", "year", "is_weekend"])
    
    print("\nDATE DIMENSION:")
    dim_date.show()
    
    # Customer Dimension
    dim_customer = spark.createDataFrame([
        (1, "C001", "Alice", "Smith", "alice@email.com", "New York", "NY", "USA", "Enterprise"),
        (2, "C002", "Bob", "Johnson", "bob@email.com", "Los Angeles", "CA", "USA", "SMB"),
        (3, "C003", "Charlie", "Brown", "charlie@email.com", "Chicago", "IL", "USA", "Consumer")
    ], ["customer_key", "customer_id", "first_name", "last_name", "email", "city", "state", "country", "segment"])
    
    print("CUSTOMER DIMENSION:")
    dim_customer.show()
    
    # Product Dimension with hierarchy
    dim_product = spark.createDataFrame([
        (1, "P001", "Laptop Pro", "Electronics", "Computers", "TechBrand", 999.99),
        (2, "P002", "Wireless Mouse", "Electronics", "Accessories", "TechBrand", 29.99),
        (3, "P003", "Cotton T-Shirt", "Clothing", "Tops", "FashionCo", 19.99)
    ], ["product_key", "product_id", "product_name", "category", "subcategory", "brand", "unit_price"])
    
    print("PRODUCT DIMENSION (with hierarchy):")
    dim_product.show()
    
    return dim_date, dim_customer, dim_product


def demonstrate_hierarchies(spark, dim_product):
    """Demonstrate dimension hierarchies."""
    print("\n" + "=" * 60)
    print("DIMENSION HIERARCHIES")
    print("=" * 60)
    
    dim_product.createOrReplaceTempView("dim_product")
    
    print("\nDrill-down: Category -> Subcategory -> Product")
    spark.sql("""
        SELECT category, subcategory, product_name, unit_price
        FROM dim_product
        ORDER BY category, subcategory, product_name
    """).show()


def main():
    spark = SparkSession.builder.appName("DimensionTables").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    
    dim_date, dim_customer, dim_product = create_dimensions(spark)
    demonstrate_hierarchies(spark, dim_product)
    
    spark.stop()
    print("\n" + "=" * 60)
    print("ALL DEMONSTRATIONS COMPLETED!")
    print("=" * 60)


if __name__ == "__main__":
    main()
