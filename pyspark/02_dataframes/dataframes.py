"""
Spark DataFrames - PySpark Learning Hub
=======================================
This module demonstrates DataFrame operations in PySpark.
DataFrames are distributed collections of data organized into named columns.
"""

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, ArrayType, MapType
from pyspark.sql.functions import col, lit, when, expr, upper, lower, length, concat
from pyspark.sql.functions import round as spark_round, floor, ceil
from pyspark.sql.functions import year, month, dayofmonth, date_format, current_date, datediff
from pyspark.sql.functions import size, array_contains, explode


def create_spark_session(app_name: str = "DataFrame Basics") -> SparkSession:
    """Create and return a SparkSession."""
    return SparkSession.builder \
        .appName(app_name) \
        .master("local[*]") \
        .getOrCreate()


def demonstrate_dataframe_creation(spark: SparkSession) -> None:
    """Demonstrate various ways to create DataFrames."""
    print("\n" + "=" * 60)
    print("DATAFRAME CREATION METHODS")
    print("=" * 60)
    
    # Method 1: From a list of tuples
    data = [
        ("Alice", 25, "Engineering", 50000.0),
        ("Bob", 30, "Marketing", 60000.0),
        ("Charlie", 35, "Engineering", 75000.0),
        ("Diana", 28, "Sales", 55000.0)
    ]
    columns = ["name", "age", "department", "salary"]
    df1 = spark.createDataFrame(data, columns)
    print("\n1. DataFrame from list of tuples:")
    df1.show()
    
    # Method 2: From a list of dictionaries
    data_dict = [
        {"name": "Alice", "age": 25, "salary": 50000.0},
        {"name": "Bob", "age": 30, "salary": 60000.0}
    ]
    df2 = spark.createDataFrame(data_dict)
    print("2. DataFrame from list of dictionaries:")
    df2.show()
    
    # Method 3: With explicit schema
    schema = StructType([
        StructField("name", StringType(), True),
        StructField("age", IntegerType(), True),
        StructField("department", StringType(), True),
        StructField("salary", DoubleType(), True)
    ])
    df3 = spark.createDataFrame(data, schema)
    print("3. DataFrame with explicit schema:")
    df3.printSchema()
    
    # Method 4: From RDD
    rdd = spark.sparkContext.parallelize(data)
    df4 = rdd.toDF(columns)
    print("4. DataFrame from RDD:")
    df4.show()
    
    # Method 5: Using range
    df5 = spark.range(0, 10, 2)
    print("5. DataFrame from range:")
    df5.show()


def demonstrate_schema_operations(spark: SparkSession) -> None:
    """Demonstrate schema-related operations."""
    print("\n" + "=" * 60)
    print("SCHEMA OPERATIONS")
    print("=" * 60)
    
    # Complex schema with nested types
    schema = StructType([
        StructField("id", IntegerType(), False),
        StructField("name", StringType(), True),
        StructField("skills", ArrayType(StringType()), True),
        StructField("metadata", MapType(StringType(), StringType()), True)
    ])
    
    data = [
        (1, "Alice", ["Python", "Spark"], {"level": "senior"}),
        (2, "Bob", ["Java", "Scala"], {"level": "junior"})
    ]
    
    df = spark.createDataFrame(data, schema)
    
    print("\n1. Complex schema:")
    df.printSchema()
    
    print(f"2. Column names: {df.columns}")
    print(f"3. Data types: {df.dtypes}")
    print(f"4. Number of columns: {len(df.columns)}")
    
    # Describe statistics
    print("\n5. Statistics:")
    df.describe().show()


def demonstrate_selection_filtering(spark: SparkSession) -> None:
    """Demonstrate selection and filtering operations."""
    print("\n" + "=" * 60)
    print("SELECTION AND FILTERING")
    print("=" * 60)
    
    data = [
        ("Alice", 25, "Engineering", 50000),
        ("Bob", 30, "Marketing", 60000),
        ("Charlie", 35, "Engineering", 75000),
        ("Diana", 28, "Sales", 55000),
        ("Eve", 32, "Engineering", 80000)
    ]
    df = spark.createDataFrame(data, ["name", "age", "department", "salary"])
    
    # Select columns
    print("\n1. Select specific columns:")
    df.select("name", "salary").show()
    
    # Select with expressions
    print("2. Select with expressions:")
    df.select(
        col("name"),
        (col("salary") / 12).alias("monthly_salary")
    ).show()
    
    # Filter rows
    print("3. Filter by condition:")
    df.filter(col("age") > 28).show()
    
    # Multiple conditions
    print("4. Multiple conditions (AND):")
    df.filter((col("age") > 25) & (col("salary") > 55000)).show()
    
    print("5. Multiple conditions (OR):")
    df.filter((col("department") == "Engineering") | (col("department") == "Sales")).show()
    
    # Pattern matching
    print("6. LIKE pattern matching:")
    df.filter(col("name").like("A%")).show()
    
    # IN clause
    print("7. IN clause:")
    df.filter(col("department").isin("Engineering", "Sales")).show()
    
    # BETWEEN
    print("8. BETWEEN:")
    df.filter(col("age").between(25, 32)).show()
    
    # Conditional column
    print("9. Conditional column (WHEN):")
    df.select(
        col("name"),
        col("salary"),
        when(col("salary") > 70000, "High")
        .when(col("salary") > 55000, "Medium")
        .otherwise("Low").alias("salary_level")
    ).show()
    
    # Distinct
    print("10. Distinct departments:")
    df.select("department").distinct().show()


def demonstrate_transformations(spark: SparkSession) -> None:
    """Demonstrate DataFrame transformations."""
    print("\n" + "=" * 60)
    print("DATAFRAME TRANSFORMATIONS")
    print("=" * 60)
    
    data = [
        ("Alice", 25, "Engineering", 50000),
        ("Bob", 30, "Marketing", 60000),
        ("Charlie", 35, "Engineering", 75000)
    ]
    df = spark.createDataFrame(data, ["name", "age", "department", "salary"])
    
    # Add column
    print("\n1. Add column:")
    df_with_bonus = df.withColumn("bonus", col("salary") * 0.1)
    df_with_bonus.show()
    
    # Rename column
    print("2. Rename column:")
    df.withColumnRenamed("name", "employee_name").show()
    
    # Drop column
    print("3. Drop column:")
    df.drop("department").show()
    
    # String functions
    print("4. String functions:")
    df.select(
        upper(col("name")).alias("upper_name"),
        lower(col("department")).alias("lower_dept"),
        length(col("name")).alias("name_length")
    ).show()
    
    # Numeric functions
    print("5. Numeric functions:")
    df.select(
        col("salary"),
        spark_round(col("salary") / 12, 2).alias("monthly"),
        floor(col("salary") / 1000).alias("thousands")
    ).show()
    
    # Sorting
    print("6. Sort by salary (descending):")
    df.orderBy(col("salary").desc()).show()
    
    # Limit
    print("7. Limit to 2 rows:")
    df.limit(2).show()


def demonstrate_aggregations(spark: SparkSession) -> None:
    """Demonstrate aggregation operations."""
    print("\n" + "=" * 60)
    print("AGGREGATIONS")
    print("=" * 60)
    
    from pyspark.sql.functions import count, sum as spark_sum, avg, min as spark_min, max as spark_max
    
    data = [
        ("Alice", 25, "Engineering", 50000),
        ("Bob", 30, "Marketing", 60000),
        ("Charlie", 35, "Engineering", 75000),
        ("Diana", 28, "Sales", 55000),
        ("Eve", 32, "Engineering", 80000)
    ]
    df = spark.createDataFrame(data, ["name", "age", "department", "salary"])
    
    # Basic aggregations
    print("\n1. Basic aggregations:")
    df.select(
        count("*").alias("total_count"),
        spark_sum("salary").alias("total_salary"),
        avg("salary").alias("avg_salary"),
        spark_min("salary").alias("min_salary"),
        spark_max("salary").alias("max_salary")
    ).show()
    
    # Group by
    print("2. Group by department:")
    df.groupBy("department").agg(
        count("*").alias("count"),
        avg("salary").alias("avg_salary"),
        spark_sum("salary").alias("total_salary")
    ).show()
    
    # Multiple grouping columns
    print("3. Group by multiple columns:")
    df.groupBy("department").agg(
        spark_min("age").alias("min_age"),
        spark_max("age").alias("max_age")
    ).show()


def demonstrate_joins(spark: SparkSession) -> None:
    """Demonstrate join operations."""
    print("\n" + "=" * 60)
    print("JOIN OPERATIONS")
    print("=" * 60)
    
    # Create two DataFrames
    employees = spark.createDataFrame([
        (1, "Alice", 101),
        (2, "Bob", 102),
        (3, "Charlie", 101),
        (4, "Diana", 103)
    ], ["emp_id", "name", "dept_id"])
    
    departments = spark.createDataFrame([
        (101, "Engineering"),
        (102, "Marketing"),
        (104, "HR")
    ], ["dept_id", "dept_name"])
    
    print("\nEmployees:")
    employees.show()
    print("Departments:")
    departments.show()
    
    # Inner join
    print("1. Inner Join:")
    employees.join(departments, "dept_id", "inner").show()
    
    # Left join
    print("2. Left Join:")
    employees.join(departments, "dept_id", "left").show()
    
    # Right join
    print("3. Right Join:")
    employees.join(departments, "dept_id", "right").show()
    
    # Full outer join
    print("4. Full Outer Join:")
    employees.join(departments, "dept_id", "outer").show()
    
    # Cross join
    print("5. Cross Join (limited):")
    employees.limit(2).crossJoin(departments.limit(2)).show()


def demonstrate_io_operations(spark: SparkSession) -> None:
    """Demonstrate reading and writing DataFrames."""
    print("\n" + "=" * 60)
    print("I/O OPERATIONS")
    print("=" * 60)
    
    data = [
        ("Alice", 25, "Engineering", 50000),
        ("Bob", 30, "Marketing", 60000),
        ("Charlie", 35, "Engineering", 75000)
    ]
    df = spark.createDataFrame(data, ["name", "age", "department", "salary"])
    
    # Write to different formats
    print("\n1. Writing to CSV...")
    df.write.mode("overwrite").csv("/tmp/employees_csv", header=True)
    
    print("2. Writing to JSON...")
    df.write.mode("overwrite").json("/tmp/employees_json")
    
    print("3. Writing to Parquet...")
    df.write.mode("overwrite").parquet("/tmp/employees_parquet")
    
    # Read back
    print("\n4. Reading from CSV:")
    spark.read.csv("/tmp/employees_csv", header=True, inferSchema=True).show()
    
    print("5. Reading from JSON:")
    spark.read.json("/tmp/employees_json").show()
    
    print("6. Reading from Parquet:")
    spark.read.parquet("/tmp/employees_parquet").show()


def main():
    """Main function to run all demonstrations."""
    print("\n" + "=" * 60)
    print("PYSPARK DATAFRAMES - COMPREHENSIVE GUIDE")
    print("=" * 60)
    
    spark = create_spark_session()
    
    try:
        demonstrate_dataframe_creation(spark)
        demonstrate_schema_operations(spark)
        demonstrate_selection_filtering(spark)
        demonstrate_transformations(spark)
        demonstrate_aggregations(spark)
        demonstrate_joins(spark)
        demonstrate_io_operations(spark)
        
        print("\n" + "=" * 60)
        print("ALL DEMONSTRATIONS COMPLETED SUCCESSFULLY!")
        print("=" * 60)
        
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
