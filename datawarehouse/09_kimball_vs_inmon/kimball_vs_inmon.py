"""
Kimball vs Inmon - DW Learning Hub
==================================
This module demonstrates Kimball and Inmon approaches using PySpark.
"""

from pyspark.sql import SparkSession


def demonstrate_kimball_approach(spark):
    """Demonstrate Kimball dimensional modeling."""
    print("\n" + "=" * 60)
    print("KIMBALL APPROACH (Bottom-Up)")
    print("=" * 60)
    
    print("\nKimball's Four-Step Process:")
    print("1. Select business process (e.g., Sales)")
    print("2. Declare grain (one row per transaction)")
    print("3. Identify dimensions (Date, Product, Customer)")
    print("4. Identify facts (Quantity, Amount, Profit)")
    
    # Create dimensional model
    dim_product = spark.createDataFrame([
        (1, "Laptop", "Electronics", "TechBrand"),
        (2, "T-Shirt", "Clothing", "FashionCo")
    ], ["product_key", "product_name", "category", "brand"])
    
    fact_sales = spark.createDataFrame([
        (20240115, 1, 1, 2, 1999.98),
        (20240115, 2, 2, 5, 149.95)
    ], ["date_key", "product_key", "customer_key", "quantity", "total_amount"])
    
    print("\nDimensional Model (Star Schema):")
    print("Dimension: Product")
    dim_product.show()
    print("Fact: Sales")
    fact_sales.show()
    
    print("Key Concepts:")
    print("- Conformed Dimensions: Same dimensions across data marts")
    print("- Bus Matrix: Documents dimension/fact relationships")
    print("- Grain: Level of detail in fact table")


def demonstrate_inmon_approach(spark):
    """Demonstrate Inmon enterprise DW approach."""
    print("\n" + "=" * 60)
    print("INMON APPROACH (Top-Down)")
    print("=" * 60)
    
    print("\nInmon's Characteristics:")
    print("1. Subject-oriented")
    print("2. Integrated")
    print("3. Time-variant")
    print("4. Non-volatile")
    
    # Create normalized model (3NF)
    category = spark.createDataFrame([
        (1, "Electronics", "Technology"),
        (2, "Clothing", "Apparel")
    ], ["category_id", "category_name", "department"])
    
    product = spark.createDataFrame([
        (1, "Laptop", 1),
        (2, "T-Shirt", 2)
    ], ["product_id", "product_name", "category_id"])
    
    print("\nNormalized Model (3NF):")
    print("Table: Category")
    category.show()
    print("Table: Product (references Category)")
    product.show()
    
    print("Key Concepts:")
    print("- Central enterprise data warehouse")
    print("- Normalized design (3NF)")
    print("- Data marts derived from central DW")


def main():
    spark = SparkSession.builder.appName("KimballVsInmon").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    
    demonstrate_kimball_approach(spark)
    demonstrate_inmon_approach(spark)
    
    spark.stop()
    print("\n" + "=" * 60)
    print("ALL DEMONSTRATIONS COMPLETED!")
    print("=" * 60)


if __name__ == "__main__":
    main()
