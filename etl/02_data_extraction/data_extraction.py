"""
Data Extraction - ETL Learning Hub
==================================
This module demonstrates data extraction techniques using PySpark.
"""

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType


def demonstrate_file_extraction(spark):
    """Demonstrate file-based extraction."""
    print("\n" + "=" * 60)
    print("FILE-BASED EXTRACTION")
    print("=" * 60)
    
    # Create sample data to simulate file extraction
    data = [
        (1, "Alice", 50000.0),
        (2, "Bob", 60000.0),
        (3, "Charlie", 75000.0)
    ]
    
    schema = StructType([
        StructField("id", IntegerType(), True),
        StructField("name", StringType(), True),
        StructField("salary", DoubleType(), True)
    ])
    
    df = spark.createDataFrame(data, schema)
    
    print("\nExtracted data with schema:")
    df.printSchema()
    df.show()
    
    print("\nExtraction options:")
    print("- CSV: spark.read.csv('path', header=True, inferSchema=True)")
    print("- JSON: spark.read.json('path', multiLine=True)")
    print("- Parquet: spark.read.parquet('path')")
    print("- Delta: spark.read.format('delta').load('path')")


def demonstrate_database_extraction(spark):
    """Demonstrate database extraction concepts."""
    print("\n" + "=" * 60)
    print("DATABASE EXTRACTION")
    print("=" * 60)
    
    print("\nJDBC extraction pattern:")
    print("""
    df = spark.read \\
        .format("jdbc") \\
        .option("url", "jdbc:postgresql://host:5432/db") \\
        .option("dbtable", "schema.table") \\
        .option("user", "username") \\
        .option("password", "password") \\
        .load()
    """)
    
    print("\nParallel extraction for large tables:")
    print("""
    df = spark.read \\
        .format("jdbc") \\
        .option("partitionColumn", "id") \\
        .option("lowerBound", "1") \\
        .option("upperBound", "1000000") \\
        .option("numPartitions", "10") \\
        .load()
    """)


def main():
    spark = SparkSession.builder.appName("DataExtraction").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    
    demonstrate_file_extraction(spark)
    demonstrate_database_extraction(spark)
    
    spark.stop()
    print("\n" + "=" * 60)
    print("ALL DEMONSTRATIONS COMPLETED!")
    print("=" * 60)


if __name__ == "__main__":
    main()
