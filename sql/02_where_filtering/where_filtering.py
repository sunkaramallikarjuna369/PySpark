"""
WHERE & Filtering - SQL Learning Hub
====================================
This module demonstrates WHERE clause using PySpark SQL.
"""

from pyspark.sql import SparkSession


def demonstrate_where_filtering(spark):
    """Demonstrate WHERE clause filtering."""
    print("\n" + "=" * 60)
    print("WHERE & FILTERING")
    print("=" * 60)
    
    data = [
        (1, "Alice", 50000, 101),
        (2, "Bob", 60000, 102),
        (3, "Charlie", 75000, 101),
        (4, "Diana", 55000, 103),
        (5, "Eve", 45000, 102)
    ]
    df = spark.createDataFrame(data, ["id", "name", "salary", "dept_id"])
    df.createOrReplaceTempView("employees")
    
    print("\nWHERE salary > 50000:")
    spark.sql("SELECT * FROM employees WHERE salary > 50000").show()
    
    print("\nWHERE with AND:")
    spark.sql("SELECT * FROM employees WHERE salary > 50000 AND dept_id = 101").show()
    
    print("\nWHERE with IN:")
    spark.sql("SELECT * FROM employees WHERE dept_id IN (101, 102)").show()
    
    print("\nWHERE with LIKE:")
    spark.sql("SELECT * FROM employees WHERE name LIKE 'A%'").show()


def main():
    spark = SparkSession.builder.appName("WhereFiltering").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    demonstrate_where_filtering(spark)
    spark.stop()


if __name__ == "__main__":
    main()
