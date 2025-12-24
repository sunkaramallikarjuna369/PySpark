"""
ORDER BY - SQL Learning Hub
===========================
This module demonstrates ORDER BY using PySpark SQL.
"""

from pyspark.sql import SparkSession


def demonstrate_order_by(spark):
    """Demonstrate ORDER BY operations."""
    print("\n" + "=" * 60)
    print("ORDER BY")
    print("=" * 60)
    
    data = [
        (1, "Alice", 50000, 101), (2, "Bob", 60000, 102),
        (3, "Charlie", 75000, 101), (4, "Diana", 55000, 103)
    ]
    df = spark.createDataFrame(data, ["id", "name", "salary", "dept_id"])
    df.createOrReplaceTempView("employees")
    
    print("\nORDER BY salary ASC:")
    spark.sql("SELECT * FROM employees ORDER BY salary ASC").show()
    
    print("\nORDER BY salary DESC:")
    spark.sql("SELECT * FROM employees ORDER BY salary DESC").show()
    
    print("\nMultiple columns:")
    spark.sql("SELECT * FROM employees ORDER BY dept_id ASC, salary DESC").show()


def main():
    spark = SparkSession.builder.appName("OrderBy").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    demonstrate_order_by(spark)
    spark.stop()


if __name__ == "__main__":
    main()
