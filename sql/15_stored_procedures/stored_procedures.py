"""
Stored Procedures - SQL Learning Hub
====================================
This module demonstrates stored procedure concepts using PySpark SQL UDFs.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import DoubleType, StringType


def demonstrate_udfs(spark):
    """Demonstrate UDFs as Spark's equivalent to stored procedures."""
    print("\n" + "=" * 60)
    print("STORED PROCEDURES (UDFs in Spark)")
    print("=" * 60)
    
    data = [
        (1, "Alice", 50000, 85),
        (2, "Bob", 60000, 90),
        (3, "Charlie", 75000, 75),
        (4, "Diana", 55000, 95)
    ]
    df = spark.createDataFrame(data, ["id", "name", "salary", "performance"])
    df.createOrReplaceTempView("employees")
    
    # Register UDF (similar to stored function)
    def calculate_bonus(salary, performance):
        if performance >= 90:
            return salary * 0.20
        elif performance >= 80:
            return salary * 0.15
        else:
            return salary * 0.10
    
    spark.udf.register("calculate_bonus", calculate_bonus, DoubleType())
    
    print("\nUsing UDF (calculate_bonus):")
    spark.sql("""
        SELECT name, salary, performance,
               calculate_bonus(salary, performance) AS bonus
        FROM employees
    """).show()
    
    # Another UDF
    def get_performance_level(score):
        if score >= 90:
            return "Excellent"
        elif score >= 80:
            return "Good"
        elif score >= 70:
            return "Average"
        else:
            return "Needs Improvement"
    
    spark.udf.register("get_performance_level", get_performance_level, StringType())
    
    print("\nUsing UDF (get_performance_level):")
    spark.sql("""
        SELECT name, performance,
               get_performance_level(performance) AS level
        FROM employees
    """).show()


def main():
    spark = SparkSession.builder.appName("StoredProcedures").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    demonstrate_udfs(spark)
    spark.stop()


if __name__ == "__main__":
    main()
