"""
Indexes & Performance - SQL Learning Hub
========================================
This module demonstrates index concepts using PySpark SQL.
"""

from pyspark.sql import SparkSession


def demonstrate_performance(spark):
    """Demonstrate performance concepts."""
    print("\n" + "=" * 60)
    print("INDEXES & PERFORMANCE")
    print("=" * 60)
    
    # Create sample data
    data = [(i, f"Employee_{i}", i * 1000, i % 10) for i in range(1, 101)]
    df = spark.createDataFrame(data, ["id", "name", "salary", "dept_id"])
    df.createOrReplaceTempView("employees")
    
    print("\nQuery execution plan (EXPLAIN):")
    spark.sql("EXPLAIN SELECT * FROM employees WHERE dept_id = 5").show(truncate=False)
    
    print("\nNote: In Spark, partitioning and bucketing serve similar purposes to indexes")
    print("- Partitioning: Divides data by column values")
    print("- Bucketing: Hashes data into fixed number of buckets")
    print("- Both help with query optimization and data locality")


def main():
    spark = SparkSession.builder.appName("IndexesPerformance").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    demonstrate_performance(spark)
    spark.stop()


if __name__ == "__main__":
    main()
