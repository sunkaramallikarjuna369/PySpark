"""
Broadcast Variables - PySpark Learning Hub
==========================================
This module demonstrates broadcast variables in PySpark.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import broadcast, col, udf
from pyspark.sql.types import StringType


def create_spark_session():
    return SparkSession.builder.appName("Broadcast Variables").getOrCreate()


def demonstrate_basic_broadcast(spark):
    """Demonstrate basic broadcast variable usage."""
    print("\n" + "=" * 60)
    print("BASIC BROADCAST VARIABLES")
    print("=" * 60)
    
    sc = spark.sparkContext
    
    country_codes = {"US": "United States", "UK": "United Kingdom", "CA": "Canada"}
    broadcast_countries = sc.broadcast(country_codes)
    
    data = [("Alice", "US"), ("Bob", "UK"), ("Charlie", "CA")]
    rdd = sc.parallelize(data)
    
    def lookup_country(record):
        name, code = record
        return (name, code, broadcast_countries.value.get(code, "Unknown"))
    
    result = rdd.map(lookup_country).collect()
    print("\nResults with broadcast lookup:")
    for r in result:
        print(f"  {r}")
    
    broadcast_countries.unpersist()


def demonstrate_broadcast_join(spark):
    """Demonstrate broadcast join."""
    print("\n" + "=" * 60)
    print("BROADCAST JOIN")
    print("=" * 60)
    
    sales = spark.createDataFrame([
        (1, 101, 1000.0), (2, 102, 1500.0), (3, 101, 800.0)
    ], ["sale_id", "product_id", "amount"])
    
    products = spark.createDataFrame([
        (101, "Laptop"), (102, "Phone")
    ], ["product_id", "product_name"])
    
    print("\nBroadcast Join Result:")
    sales.join(broadcast(products), "product_id").show()


def main():
    spark = create_spark_session()
    try:
        demonstrate_basic_broadcast(spark)
        demonstrate_broadcast_join(spark)
        print("\n" + "=" * 60)
        print("ALL DEMONSTRATIONS COMPLETED!")
        print("=" * 60)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
