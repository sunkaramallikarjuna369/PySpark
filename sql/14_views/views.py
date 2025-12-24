"""
Views - SQL Learning Hub
========================
This module demonstrates views using PySpark SQL.
"""

from pyspark.sql import SparkSession


def demonstrate_views(spark):
    """Demonstrate views."""
    print("\n" + "=" * 60)
    print("VIEWS")
    print("=" * 60)
    
    data = [
        (1, "Alice", 50000, 101, "Active"),
        (2, "Bob", 60000, 102, "Active"),
        (3, "Charlie", 75000, 101, "Inactive"),
        (4, "Diana", 55000, 103, "Active")
    ]
    df = spark.createDataFrame(data, ["id", "name", "salary", "dept_id", "status"])
    df.createOrReplaceTempView("employees")
    
    # Create a view
    spark.sql("""
        CREATE OR REPLACE TEMP VIEW active_employees AS
        SELECT id, name, salary, dept_id
        FROM employees
        WHERE status = 'Active'
    """)
    
    print("\nView: active_employees")
    spark.sql("SELECT * FROM active_employees").show()
    
    # Create aggregated view
    spark.sql("""
        CREATE OR REPLACE TEMP VIEW dept_stats AS
        SELECT dept_id, COUNT(*) AS count, AVG(salary) AS avg_salary
        FROM employees
        GROUP BY dept_id
    """)
    
    print("\nView: dept_stats")
    spark.sql("SELECT * FROM dept_stats").show()


def main():
    spark = SparkSession.builder.appName("Views").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    demonstrate_views(spark)
    spark.stop()


if __name__ == "__main__":
    main()
