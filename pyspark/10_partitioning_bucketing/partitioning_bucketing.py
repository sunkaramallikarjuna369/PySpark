"""
Partitioning & Bucketing - PySpark Learning Hub
================================================
This module demonstrates partitioning and bucketing in PySpark.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, spark_partition_id


def create_spark_session():
    return SparkSession.builder \
        .appName("Partitioning & Bucketing") \
        .config("spark.sql.warehouse.dir", "/tmp/spark-warehouse") \
        .getOrCreate()


def demonstrate_partitioning(spark):
    """Demonstrate data partitioning."""
    print("\n" + "=" * 60)
    print("DATA PARTITIONING")
    print("=" * 60)
    
    data = [
        ("2023-01-15", "Electronics", "USA", 1000),
        ("2023-01-20", "Clothing", "UK", 500),
        ("2023-02-10", "Electronics", "USA", 1500),
        ("2023-02-15", "Clothing", "Canada", 800)
    ]
    df = spark.createDataFrame(data, ["date", "category", "country", "amount"])
    
    print("\nWriting partitioned data by country:")
    df.write.mode("overwrite").partitionBy("country").parquet("/tmp/sales_partitioned")
    
    print("Reading with partition filter:")
    spark.read.parquet("/tmp/sales_partitioned").filter(col("country") == "USA").show()


def demonstrate_repartitioning(spark):
    """Demonstrate repartitioning operations."""
    print("\n" + "=" * 60)
    print("REPARTITIONING")
    print("=" * 60)
    
    df = spark.range(100).withColumn("category", col("id") % 5)
    
    print(f"Initial partitions: {df.rdd.getNumPartitions()}")
    
    df_repartitioned = df.repartition(10, "category")
    print(f"After repartition by category: {df_repartitioned.rdd.getNumPartitions()}")
    
    df_coalesced = df.coalesce(2)
    print(f"After coalesce(2): {df_coalesced.rdd.getNumPartitions()}")


def main():
    spark = create_spark_session()
    try:
        demonstrate_partitioning(spark)
        demonstrate_repartitioning(spark)
        print("\n" + "=" * 60)
        print("ALL DEMONSTRATIONS COMPLETED!")
        print("=" * 60)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
