"""
Subqueries - SQL Learning Hub
=============================
This module demonstrates subqueries using PySpark SQL.
"""

from pyspark.sql import SparkSession


def demonstrate_subqueries(spark):
    """Demonstrate subqueries."""
    print("\n" + "=" * 60)
    print("SUBQUERIES")
    print("=" * 60)
    
    data = [
        (1, "Alice", 50000, 101), (2, "Bob", 60000, 102),
        (3, "Charlie", 75000, 101), (4, "Diana", 55000, 103)
    ]
    df = spark.createDataFrame(data, ["id", "name", "salary", "dept_id"])
    df.createOrReplaceTempView("employees")
    
    print("\nScalar subquery in WHERE:")
    spark.sql("""
        SELECT * FROM employees
        WHERE salary > (SELECT AVG(salary) FROM employees)
    """).show()
    
    print("\nSubquery in FROM:")
    spark.sql("""
        SELECT * FROM (
            SELECT dept_id, AVG(salary) AS avg_salary
            FROM employees GROUP BY dept_id
        ) dept_avg WHERE avg_salary > 55000
    """).show()


def main():
    spark = SparkSession.builder.appName("Subqueries").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    demonstrate_subqueries(spark)
    spark.stop()


if __name__ == "__main__":
    main()
