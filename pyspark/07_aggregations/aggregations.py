"""
Aggregations - PySpark Learning Hub
===================================
This module demonstrates aggregation operations in PySpark.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    sum, avg, min, max, count, countDistinct,
    collect_list, collect_set, stddev, variance, col
)


def create_spark_session():
    return SparkSession.builder.appName("Aggregations").getOrCreate()


def demonstrate_basic_aggregations(spark):
    """Demonstrate basic aggregate functions."""
    print("\n" + "=" * 60)
    print("BASIC AGGREGATIONS")
    print("=" * 60)
    
    data = [
        ("Engineering", "Alice", 75000),
        ("Engineering", "Bob", 80000),
        ("Marketing", "Diana", 60000),
        ("Marketing", "Eve", 65000),
        ("Sales", "Frank", 55000)
    ]
    df = spark.createDataFrame(data, ["department", "name", "salary"])
    
    print("\nBasic Aggregations:")
    df.select(
        count("*").alias("total"),
        sum("salary").alias("sum"),
        avg("salary").alias("avg"),
        min("salary").alias("min"),
        max("salary").alias("max")
    ).show()


def demonstrate_groupby(spark):
    """Demonstrate groupBy operations."""
    print("\n" + "=" * 60)
    print("GROUPBY OPERATIONS")
    print("=" * 60)
    
    data = [
        ("Engineering", "Alice", 75000),
        ("Engineering", "Bob", 80000),
        ("Marketing", "Diana", 60000),
        ("Marketing", "Eve", 65000)
    ]
    df = spark.createDataFrame(data, ["department", "name", "salary"])
    
    print("\nGroupBy Department:")
    df.groupBy("department").agg(
        count("*").alias("count"),
        avg("salary").alias("avg_salary")
    ).show()


def demonstrate_pivot(spark):
    """Demonstrate pivot operations."""
    print("\n" + "=" * 60)
    print("PIVOT OPERATIONS")
    print("=" * 60)
    
    data = [
        ("Engineering", "Q1", 100000),
        ("Engineering", "Q2", 120000),
        ("Marketing", "Q1", 80000),
        ("Marketing", "Q2", 85000)
    ]
    df = spark.createDataFrame(data, ["department", "quarter", "revenue"])
    
    print("\nPivot Table:")
    df.groupBy("department").pivot("quarter").sum("revenue").show()


def main():
    spark = create_spark_session()
    try:
        demonstrate_basic_aggregations(spark)
        demonstrate_groupby(spark)
        demonstrate_pivot(spark)
        print("\n" + "=" * 60)
        print("ALL DEMONSTRATIONS COMPLETED!")
        print("=" * 60)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
