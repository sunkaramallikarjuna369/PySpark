"""
Spark Joins - PySpark Learning Hub
==================================
This module demonstrates join operations in PySpark.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import broadcast, col, coalesce, lit


def create_spark_session():
    return SparkSession.builder.appName("Spark Joins").getOrCreate()


def demonstrate_basic_joins(spark):
    """Demonstrate basic join types."""
    print("\n" + "=" * 60)
    print("BASIC JOIN TYPES")
    print("=" * 60)
    
    employees = spark.createDataFrame([
        (1, "Alice", 101), (2, "Bob", 102),
        (3, "Charlie", 101), (4, "Diana", 103), (5, "Eve", None)
    ], ["emp_id", "name", "dept_id"])
    
    departments = spark.createDataFrame([
        (101, "Engineering"), (102, "Marketing"), (104, "HR")
    ], ["dept_id", "dept_name"])
    
    print("\nEmployees:")
    employees.show()
    print("Departments:")
    departments.show()
    
    print("1. INNER JOIN:")
    employees.join(departments, "dept_id", "inner").show()
    
    print("2. LEFT OUTER JOIN:")
    employees.join(departments, "dept_id", "left").show()
    
    print("3. RIGHT OUTER JOIN:")
    employees.join(departments, "dept_id", "right").show()
    
    print("4. FULL OUTER JOIN:")
    employees.join(departments, "dept_id", "outer").show()
    
    print("5. LEFT SEMI JOIN:")
    employees.join(departments, "dept_id", "left_semi").show()
    
    print("6. LEFT ANTI JOIN:")
    employees.join(departments, "dept_id", "left_anti").show()


def demonstrate_join_optimization(spark):
    """Demonstrate join optimization techniques."""
    print("\n" + "=" * 60)
    print("JOIN OPTIMIZATION")
    print("=" * 60)
    
    large_df = spark.range(100000).withColumn("dept_id", col("id") % 100)
    small_df = spark.createDataFrame([(i, f"Dept_{i}") for i in range(100)], ["dept_id", "dept_name"])
    
    print("\n1. Broadcast Join:")
    result = large_df.join(broadcast(small_df), "dept_id")
    result.explain()
    
    print("\n2. Repartitioned Join:")
    large_repartitioned = large_df.repartition(10, "dept_id")
    result = large_repartitioned.join(small_df, "dept_id")
    result.explain()


def main():
    spark = create_spark_session()
    try:
        demonstrate_basic_joins(spark)
        demonstrate_join_optimization(spark)
        print("\n" + "=" * 60)
        print("ALL DEMONSTRATIONS COMPLETED!")
        print("=" * 60)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
