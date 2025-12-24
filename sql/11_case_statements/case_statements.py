"""
CASE Statements - SQL Learning Hub
==================================
This module demonstrates CASE statements using PySpark SQL.
"""

from pyspark.sql import SparkSession


def demonstrate_case_statements(spark):
    """Demonstrate CASE statements."""
    print("\n" + "=" * 60)
    print("CASE STATEMENTS")
    print("=" * 60)
    
    data = [
        (1, "Alice", 50000, 101), (2, "Bob", 60000, 102),
        (3, "Charlie", 75000, 101), (4, "Diana", 35000, 103)
    ]
    df = spark.createDataFrame(data, ["id", "name", "salary", "dept_id"])
    df.createOrReplaceTempView("employees")
    
    print("\nSimple CASE:")
    spark.sql("""
        SELECT name, dept_id,
               CASE dept_id
                   WHEN 101 THEN 'IT'
                   WHEN 102 THEN 'HR'
                   ELSE 'Other'
               END AS department
        FROM employees
    """).show()
    
    print("\nSearched CASE:")
    spark.sql("""
        SELECT name, salary,
               CASE 
                   WHEN salary >= 70000 THEN 'High'
                   WHEN salary >= 50000 THEN 'Medium'
                   ELSE 'Low'
               END AS salary_level
        FROM employees
    """).show()


def main():
    spark = SparkSession.builder.appName("CaseStatements").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    demonstrate_case_statements(spark)
    spark.stop()


if __name__ == "__main__":
    main()
