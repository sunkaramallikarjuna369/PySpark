"""
Window Functions - SQL Learning Hub
===================================
This module demonstrates window functions using PySpark SQL.
"""

from pyspark.sql import SparkSession


def demonstrate_window_functions(spark):
    """Demonstrate window functions."""
    print("\n" + "=" * 60)
    print("WINDOW FUNCTIONS")
    print("=" * 60)
    
    data = [
        (1, "Alice", 50000, 101), (2, "Bob", 60000, 102),
        (3, "Charlie", 75000, 101), (4, "Diana", 55000, 101),
        (5, "Eve", 45000, 102)
    ]
    df = spark.createDataFrame(data, ["id", "name", "salary", "dept_id"])
    df.createOrReplaceTempView("employees")
    
    print("\nROW_NUMBER:")
    spark.sql("""
        SELECT name, salary, dept_id,
               ROW_NUMBER() OVER (PARTITION BY dept_id ORDER BY salary DESC) AS rank
        FROM employees
    """).show()
    
    print("\nRunning total:")
    spark.sql("""
        SELECT name, salary,
               SUM(salary) OVER (ORDER BY salary) AS running_total
        FROM employees
    """).show()


def main():
    spark = SparkSession.builder.appName("WindowFunctions").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    demonstrate_window_functions(spark)
    spark.stop()


if __name__ == "__main__":
    main()
