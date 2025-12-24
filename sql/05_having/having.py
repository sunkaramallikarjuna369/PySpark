"""
HAVING Clause - SQL Learning Hub
================================
This module demonstrates HAVING clause using PySpark SQL.
"""

from pyspark.sql import SparkSession


def demonstrate_having(spark):
    """Demonstrate HAVING clause."""
    print("\n" + "=" * 60)
    print("HAVING CLAUSE")
    print("=" * 60)
    
    data = [
        (1, "Alice", 50000, 101), (2, "Bob", 60000, 102),
        (3, "Charlie", 75000, 101), (4, "Diana", 55000, 101),
        (5, "Eve", 45000, 102), (6, "Frank", 80000, 103)
    ]
    df = spark.createDataFrame(data, ["id", "name", "salary", "dept_id"])
    df.createOrReplaceTempView("employees")
    
    print("\nHAVING COUNT(*) > 2:")
    spark.sql("""
        SELECT dept_id, COUNT(*) AS count
        FROM employees
        GROUP BY dept_id
        HAVING COUNT(*) > 2
    """).show()
    
    print("\nWHERE and HAVING together:")
    spark.sql("""
        SELECT dept_id, AVG(salary) AS avg_salary
        FROM employees
        WHERE salary > 40000
        GROUP BY dept_id
        HAVING AVG(salary) > 55000
    """).show()


def main():
    spark = SparkSession.builder.appName("Having").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    demonstrate_having(spark)
    spark.stop()


if __name__ == "__main__":
    main()
