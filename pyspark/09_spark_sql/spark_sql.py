"""
Spark SQL - PySpark Learning Hub
================================
This module demonstrates Spark SQL operations.
"""

from pyspark.sql import SparkSession


def create_spark_session():
    return SparkSession.builder.appName("Spark SQL").getOrCreate()


def demonstrate_basic_sql(spark):
    """Demonstrate basic SQL operations."""
    print("\n" + "=" * 60)
    print("BASIC SPARK SQL")
    print("=" * 60)
    
    data = [
        (1, "Alice", "Engineering", 75000),
        (2, "Bob", "Marketing", 60000),
        (3, "Charlie", "Engineering", 80000),
        (4, "Diana", "Sales", 55000)
    ]
    df = spark.createDataFrame(data, ["id", "name", "department", "salary"])
    df.createOrReplaceTempView("employees")
    
    print("\nBasic SELECT:")
    spark.sql("SELECT * FROM employees").show()
    
    print("SELECT with WHERE:")
    spark.sql("SELECT name, salary FROM employees WHERE salary > 60000").show()
    
    print("GROUP BY:")
    spark.sql("""
        SELECT department, COUNT(*) as count, AVG(salary) as avg_salary
        FROM employees GROUP BY department
    """).show()


def demonstrate_advanced_sql(spark):
    """Demonstrate advanced SQL operations."""
    print("\n" + "=" * 60)
    print("ADVANCED SPARK SQL")
    print("=" * 60)
    
    employees = spark.createDataFrame([
        (1, "Alice", 101, 75000),
        (2, "Bob", 102, 60000),
        (3, "Charlie", 101, 80000)
    ], ["id", "name", "dept_id", "salary"])
    
    departments = spark.createDataFrame([
        (101, "Engineering"),
        (102, "Marketing")
    ], ["dept_id", "dept_name"])
    
    employees.createOrReplaceTempView("employees")
    departments.createOrReplaceTempView("departments")
    
    print("\nJOIN:")
    spark.sql("""
        SELECT e.name, d.dept_name, e.salary
        FROM employees e
        JOIN departments d ON e.dept_id = d.dept_id
    """).show()
    
    print("Window Function:")
    spark.sql("""
        SELECT name, salary,
               ROW_NUMBER() OVER (ORDER BY salary DESC) as rank
        FROM employees
    """).show()


def main():
    spark = create_spark_session()
    try:
        demonstrate_basic_sql(spark)
        demonstrate_advanced_sql(spark)
        print("\n" + "=" * 60)
        print("ALL DEMONSTRATIONS COMPLETED!")
        print("=" * 60)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
