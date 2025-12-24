"""
Conformed Dimensions - DW Learning Hub
======================================
This module demonstrates conformed dimension concepts using PySpark.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as spark_sum


def demonstrate_conformed_dimensions(spark):
    """Demonstrate conformed dimensions."""
    print("\n" + "=" * 60)
    print("CONFORMED DIMENSIONS")
    print("=" * 60)
    
    # Conformed Customer Dimension
    dim_customer = spark.createDataFrame([
        (1, "C001", "Alice Smith", "Enterprise", "North"),
        (2, "C002", "Bob Johnson", "SMB", "South")
    ], ["customer_key", "customer_id", "customer_name", "segment", "region"])
    
    print("\nConformed Customer Dimension:")
    dim_customer.show()
    
    # Multiple fact tables using same dimension
    fact_sales = spark.createDataFrame([
        (1, 1000.00),
        (2, 500.00)
    ], ["customer_key", "sales_amount"])
    
    fact_support = spark.createDataFrame([
        (1, 2),
        (2, 1)
    ], ["customer_key", "ticket_count"])
    
    print("Fact Sales:")
    fact_sales.show()
    
    print("Fact Support:")
    fact_support.show()


def demonstrate_drill_across(spark):
    """Demonstrate drill-across query."""
    print("\n" + "=" * 60)
    print("DRILL-ACROSS QUERY")
    print("=" * 60)
    
    dim_customer = spark.createDataFrame([
        (1, "Alice Smith", "Enterprise"),
        (2, "Bob Johnson", "SMB")
    ], ["customer_key", "customer_name", "segment"])
    
    fact_sales = spark.createDataFrame([
        (1, 1000.00),
        (2, 500.00)
    ], ["customer_key", "sales_amount"])
    
    fact_support = spark.createDataFrame([
        (1, 2),
        (2, 1)
    ], ["customer_key", "ticket_count"])
    
    # Drill-across: Combine metrics from different facts
    sales_agg = fact_sales.groupBy("customer_key").agg(
        spark_sum("sales_amount").alias("total_sales")
    )
    
    support_agg = fact_support.groupBy("customer_key").agg(
        spark_sum("ticket_count").alias("total_tickets")
    )
    
    drill_across = sales_agg.join(support_agg, "customer_key") \
        .join(dim_customer, "customer_key") \
        .select("customer_name", "segment", "total_sales", "total_tickets")
    
    print("\nDrill-across result (Sales + Support by Customer):")
    drill_across.show()
    
    print("This is only possible because dim_customer is conformed!")


def main():
    spark = SparkSession.builder.appName("ConformedDimensions").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    
    demonstrate_conformed_dimensions(spark)
    demonstrate_drill_across(spark)
    
    spark.stop()
    print("\n" + "=" * 60)
    print("ALL DEMONSTRATIONS COMPLETED!")
    print("=" * 60)


if __name__ == "__main__":
    main()
