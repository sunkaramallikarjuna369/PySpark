"""
UNION & Set Operations - SQL Learning Hub
=========================================
This module demonstrates set operations using PySpark SQL.
"""

from pyspark.sql import SparkSession


def demonstrate_set_operations(spark):
    """Demonstrate set operations."""
    print("\n" + "=" * 60)
    print("UNION & SET OPERATIONS")
    print("=" * 60)
    
    employees = spark.createDataFrame([
        (1, "Alice"), (2, "Bob"), (3, "Charlie")
    ], ["id", "name"])
    
    contractors = spark.createDataFrame([
        (3, "Charlie"), (4, "Diana"), (5, "Eve")
    ], ["id", "name"])
    
    employees.createOrReplaceTempView("employees")
    contractors.createOrReplaceTempView("contractors")
    
    print("\nUNION (removes duplicates):")
    spark.sql("""
        SELECT * FROM employees
        UNION
        SELECT * FROM contractors
    """).show()
    
    print("\nUNION ALL (keeps duplicates):")
    spark.sql("""
        SELECT * FROM employees
        UNION ALL
        SELECT * FROM contractors
    """).show()
    
    print("\nINTERSECT:")
    spark.sql("""
        SELECT * FROM employees
        INTERSECT
        SELECT * FROM contractors
    """).show()
    
    print("\nEXCEPT:")
    spark.sql("""
        SELECT * FROM employees
        EXCEPT
        SELECT * FROM contractors
    """).show()


def main():
    spark = SparkSession.builder.appName("SetOperations").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    demonstrate_set_operations(spark)
    spark.stop()


if __name__ == "__main__":
    main()
