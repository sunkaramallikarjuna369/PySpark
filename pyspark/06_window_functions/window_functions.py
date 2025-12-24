"""
Window Functions - PySpark Learning Hub
=======================================
This module demonstrates window functions in PySpark.
"""

from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import (
    row_number, rank, dense_rank, ntile,
    lead, lag, first, last,
    sum, avg, min, max, count, col
)


def create_spark_session():
    return SparkSession.builder.appName("Window Functions").getOrCreate()


def demonstrate_ranking_functions(spark):
    """Demonstrate ranking window functions."""
    print("\n" + "=" * 60)
    print("RANKING WINDOW FUNCTIONS")
    print("=" * 60)
    
    data = [
        ("Engineering", "Alice", 75000),
        ("Engineering", "Bob", 80000),
        ("Engineering", "Charlie", 75000),
        ("Marketing", "Diana", 60000),
        ("Marketing", "Eve", 65000)
    ]
    df = spark.createDataFrame(data, ["department", "name", "salary"])
    
    window_spec = Window.partitionBy("department").orderBy(col("salary").desc())
    
    print("\nAll ranking functions:")
    df.select(
        "department", "name", "salary",
        row_number().over(window_spec).alias("row_num"),
        rank().over(window_spec).alias("rank"),
        dense_rank().over(window_spec).alias("dense_rank"),
        ntile(2).over(window_spec).alias("ntile_2")
    ).show()


def demonstrate_analytic_functions(spark):
    """Demonstrate analytic window functions."""
    print("\n" + "=" * 60)
    print("ANALYTIC WINDOW FUNCTIONS")
    print("=" * 60)
    
    data = [
        ("2023-01-01", "A", 100),
        ("2023-01-02", "A", 120),
        ("2023-01-03", "A", 115),
        ("2023-01-04", "A", 130)
    ]
    df = spark.createDataFrame(data, ["date", "product", "sales"])
    
    window_spec = Window.partitionBy("product").orderBy("date")
    
    print("\nLEAD and LAG:")
    df.select(
        "date", "sales",
        lag("sales", 1).over(window_spec).alias("prev_day"),
        lead("sales", 1).over(window_spec).alias("next_day")
    ).show()


def demonstrate_aggregate_windows(spark):
    """Demonstrate aggregate window functions."""
    print("\n" + "=" * 60)
    print("AGGREGATE WINDOW FUNCTIONS")
    print("=" * 60)
    
    data = [
        ("2023-01-01", "A", 100),
        ("2023-01-02", "A", 150),
        ("2023-01-03", "A", 120),
        ("2023-01-04", "A", 180)
    ]
    df = spark.createDataFrame(data, ["date", "product", "sales"])
    
    window_running = Window.partitionBy("product").orderBy("date") \
        .rowsBetween(Window.unboundedPreceding, Window.currentRow)
    
    print("\nRunning aggregates:")
    df.select(
        "date", "sales",
        sum("sales").over(window_running).alias("running_sum"),
        avg("sales").over(window_running).alias("running_avg")
    ).show()


def main():
    spark = create_spark_session()
    try:
        demonstrate_ranking_functions(spark)
        demonstrate_analytic_functions(spark)
        demonstrate_aggregate_windows(spark)
        print("\n" + "=" * 60)
        print("ALL DEMONSTRATIONS COMPLETED!")
        print("=" * 60)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
