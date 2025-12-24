"""
NULL Handling - SQL Learning Hub
================================
This module demonstrates NULL handling using PySpark SQL.
"""

from pyspark.sql import SparkSession


def demonstrate_null_handling(spark):
    """Demonstrate NULL handling."""
    print("\n" + "=" * 60)
    print("NULL HANDLING")
    print("=" * 60)
    
    data = [
        (1, "Alice", 50000, 5000),
        (2, "Bob", 60000, None),
        (3, "Charlie", 75000, 7500),
        (4, "Diana", 55000, None)
    ]
    df = spark.createDataFrame(data, ["id", "name", "salary", "bonus"])
    df.createOrReplaceTempView("employees")
    
    print("\nIS NULL:")
    spark.sql("SELECT * FROM employees WHERE bonus IS NULL").show()
    
    print("\nIS NOT NULL:")
    spark.sql("SELECT * FROM employees WHERE bonus IS NOT NULL").show()
    
    print("\nCOALESCE:")
    spark.sql("""
        SELECT name, salary, bonus,
               COALESCE(bonus, 0) AS bonus_or_zero
        FROM employees
    """).show()


def main():
    spark = SparkSession.builder.appName("NullHandling").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    demonstrate_null_handling(spark)
    spark.stop()


if __name__ == "__main__":
    main()
