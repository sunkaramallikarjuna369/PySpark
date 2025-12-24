"""
GROUP BY - SQL Learning Hub
===========================
This module demonstrates GROUP BY using PySpark SQL.
"""

from pyspark.sql import SparkSession


def demonstrate_group_by(spark):
    """Demonstrate GROUP BY operations."""
    print("\n" + "=" * 60)
    print("GROUP BY")
    print("=" * 60)
    
    data = [
        (1, "Alice", 50000, 101),
        (2, "Bob", 60000, 102),
        (3, "Charlie", 75000, 101),
        (4, "Diana", 55000, 101),
        (5, "Eve", 45000, 102)
    ]
    df = spark.createDataFrame(data, ["id", "name", "salary", "dept_id"])
    df.createOrReplaceTempView("employees")
    
    print("\nCOUNT by department:")
    spark.sql("""
        SELECT dept_id, COUNT(*) AS count
        FROM employees
        GROUP BY dept_id
    """).show()
    
    print("\nMultiple aggregates:")
    spark.sql("""
        SELECT dept_id, 
               COUNT(*) AS count,
               SUM(salary) AS total_salary,
               AVG(salary) AS avg_salary
        FROM employees
        GROUP BY dept_id
    """).show()


def main():
    spark = SparkSession.builder.appName("GroupBy").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    demonstrate_group_by(spark)
    spark.stop()


if __name__ == "__main__":
    main()
