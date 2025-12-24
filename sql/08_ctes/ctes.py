"""
CTEs (Common Table Expressions) - SQL Learning Hub
=================================================
This module demonstrates CTEs using PySpark SQL.
"""

from pyspark.sql import SparkSession


def demonstrate_ctes(spark):
    """Demonstrate CTEs."""
    print("\n" + "=" * 60)
    print("CTEs (Common Table Expressions)")
    print("=" * 60)
    
    data = [
        (1, "Alice", 50000, 101), (2, "Bob", 60000, 102),
        (3, "Charlie", 75000, 101), (4, "Diana", 55000, 103)
    ]
    df = spark.createDataFrame(data, ["id", "name", "salary", "dept_id"])
    df.createOrReplaceTempView("employees")
    
    print("\nBasic CTE:")
    spark.sql("""
        WITH dept_stats AS (
            SELECT dept_id, AVG(salary) AS avg_salary
            FROM employees GROUP BY dept_id
        )
        SELECT * FROM dept_stats WHERE avg_salary > 55000
    """).show()
    
    print("\nMultiple CTEs:")
    spark.sql("""
        WITH 
        high_earners AS (SELECT * FROM employees WHERE salary > 55000),
        dept_counts AS (SELECT dept_id, COUNT(*) AS cnt FROM high_earners GROUP BY dept_id)
        SELECT * FROM dept_counts
    """).show()


def main():
    spark = SparkSession.builder.appName("CTEs").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    demonstrate_ctes(spark)
    spark.stop()


if __name__ == "__main__":
    main()
