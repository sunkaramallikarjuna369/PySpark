"""
SELECT Basics - SQL Learning Hub
================================
This module demonstrates SELECT basics using PySpark SQL.
"""

from pyspark.sql import SparkSession


def demonstrate_select_basics(spark):
    """Demonstrate SELECT basics."""
    print("\n" + "=" * 60)
    print("SELECT BASICS")
    print("=" * 60)
    
    # Create sample data
    data = [
        (1, "Alice", "Smith", 50000, 101),
        (2, "Bob", "Johnson", 60000, 102),
        (3, "Charlie", "Brown", 75000, 101),
        (4, "Diana", "Prince", 55000, 103)
    ]
    df = spark.createDataFrame(data, ["id", "first_name", "last_name", "salary", "dept_id"])
    df.createOrReplaceTempView("employees")
    
    # SELECT *
    print("\nSELECT * FROM employees:")
    spark.sql("SELECT * FROM employees").show()
    
    # SELECT specific columns
    print("\nSELECT first_name, salary FROM employees:")
    spark.sql("SELECT first_name, salary FROM employees").show()
    
    # SELECT DISTINCT
    print("\nSELECT DISTINCT dept_id FROM employees:")
    spark.sql("SELECT DISTINCT dept_id FROM employees").show()
    
    # SELECT with alias
    print("\nSELECT with aliases:")
    spark.sql("""
        SELECT 
            first_name AS name,
            salary * 12 AS annual_salary
        FROM employees
    """).show()


def main():
    spark = SparkSession.builder.appName("SelectBasics").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    
    demonstrate_select_basics(spark)
    
    spark.stop()
    print("\n" + "=" * 60)
    print("ALL DEMONSTRATIONS COMPLETED!")
    print("=" * 60)


if __name__ == "__main__":
    main()
