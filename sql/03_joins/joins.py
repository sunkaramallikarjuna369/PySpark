"""
JOINs - SQL Learning Hub
========================
This module demonstrates SQL JOINs using PySpark SQL.
"""

from pyspark.sql import SparkSession


def demonstrate_joins(spark):
    """Demonstrate SQL JOINs."""
    print("\n" + "=" * 60)
    print("SQL JOINS")
    print("=" * 60)
    
    employees = spark.createDataFrame([
        (1, "Alice", 101), (2, "Bob", 102), (3, "Charlie", 101), (4, "Diana", None)
    ], ["id", "name", "dept_id"])
    
    departments = spark.createDataFrame([
        (101, "IT"), (102, "HR"), (103, "Finance")
    ], ["dept_id", "dept_name"])
    
    employees.createOrReplaceTempView("employees")
    departments.createOrReplaceTempView("departments")
    
    print("\nINNER JOIN:")
    spark.sql("""
        SELECT e.name, d.dept_name
        FROM employees e
        INNER JOIN departments d ON e.dept_id = d.dept_id
    """).show()
    
    print("\nLEFT JOIN:")
    spark.sql("""
        SELECT e.name, d.dept_name
        FROM employees e
        LEFT JOIN departments d ON e.dept_id = d.dept_id
    """).show()


def main():
    spark = SparkSession.builder.appName("Joins").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    demonstrate_joins(spark)
    spark.stop()


if __name__ == "__main__":
    main()
