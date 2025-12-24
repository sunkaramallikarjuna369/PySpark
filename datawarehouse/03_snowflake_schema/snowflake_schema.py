"""
Snowflake Schema - DW Learning Hub
==================================
This module demonstrates snowflake schema design using PySpark.
"""

from pyspark.sql import SparkSession


def create_snowflake_schema(spark):
    """Create snowflake schema tables."""
    print("\n" + "=" * 60)
    print("SNOWFLAKE SCHEMA DESIGN")
    print("=" * 60)
    
    # Level 2: Country (most normalized)
    dim_country = spark.createDataFrame([
        (1, "USA", "North America"),
        (2, "Germany", "Europe"),
        (3, "Japan", "Asia")
    ], ["country_key", "country_name", "continent"])
    
    # Level 2: Category
    dim_category = spark.createDataFrame([
        (1, "Electronics", "Technology"),
        (2, "Clothing", "Apparel"),
        (3, "Home", "Lifestyle")
    ], ["category_key", "category_name", "department"])
    
    # Level 1: Brand (references Country)
    dim_brand = spark.createDataFrame([
        (1, "TechCorp", 1),
        (2, "FashionCo", 2),
        (3, "HomeStyle", 1)
    ], ["brand_key", "brand_name", "country_key"])
    
    # Level 1: Product (references Category, Brand)
    dim_product = spark.createDataFrame([
        (1, "Laptop", 1, 1, 999.99),
        (2, "T-Shirt", 2, 2, 29.99),
        (3, "Lamp", 3, 3, 49.99)
    ], ["product_key", "product_name", "category_key", "brand_key", "unit_price"])
    
    # Fact: Sales
    fact_sales = spark.createDataFrame([
        (1, 1, 2, 1999.98),
        (2, 2, 5, 149.95),
        (3, 3, 3, 149.97)
    ], ["sale_key", "product_key", "quantity", "total_amount"])
    
    print("\nNormalized Dimension: Country")
    dim_country.show()
    
    print("Normalized Dimension: Category")
    dim_category.show()
    
    print("Dimension: Brand (references Country)")
    dim_brand.show()
    
    print("Dimension: Product (references Category, Brand)")
    dim_product.show()
    
    print("Fact: Sales")
    fact_sales.show()
    
    return dim_country, dim_category, dim_brand, dim_product, fact_sales


def query_snowflake_schema(spark, dim_country, dim_category, dim_brand, dim_product, fact_sales):
    """Query snowflake schema."""
    print("\n" + "=" * 60)
    print("SNOWFLAKE SCHEMA QUERY")
    print("=" * 60)
    
    # Register tables
    dim_country.createOrReplaceTempView("dim_country")
    dim_category.createOrReplaceTempView("dim_category")
    dim_brand.createOrReplaceTempView("dim_brand")
    dim_product.createOrReplaceTempView("dim_product")
    fact_sales.createOrReplaceTempView("fact_sales")
    
    print("\nSales by Category and Brand Country:")
    spark.sql("""
        SELECT 
            c.category_name,
            co.country_name as brand_country,
            SUM(f.total_amount) as total_sales
        FROM fact_sales f
        JOIN dim_product p ON f.product_key = p.product_key
        JOIN dim_category c ON p.category_key = c.category_key
        JOIN dim_brand b ON p.brand_key = b.brand_key
        JOIN dim_country co ON b.country_key = co.country_key
        GROUP BY c.category_name, co.country_name
    """).show()


def main():
    spark = SparkSession.builder.appName("SnowflakeSchema").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    
    tables = create_snowflake_schema(spark)
    query_snowflake_schema(spark, *tables)
    
    spark.stop()
    print("\n" + "=" * 60)
    print("ALL DEMONSTRATIONS COMPLETED!")
    print("=" * 60)


if __name__ == "__main__":
    main()
