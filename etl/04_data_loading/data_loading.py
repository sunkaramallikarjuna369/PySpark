"""
Data Loading - ETL Learning Hub
===============================
This module demonstrates data loading techniques using PySpark.
"""

from pyspark.sql import SparkSession


def demonstrate_write_modes(spark):
    """Demonstrate different write modes."""
    print("\n" + "=" * 60)
    print("WRITE MODES")
    print("=" * 60)
    
    df = spark.createDataFrame([
        (1, "Alice", 50000),
        (2, "Bob", 60000),
        (3, "Charlie", 75000)
    ], ["id", "name", "salary"])
    
    print("\nSample data:")
    df.show()
    
    print("\nWrite modes:")
    print("- overwrite: Replace existing data")
    print("- append: Add to existing data")
    print("- ignore: Skip if exists")
    print("- error: Fail if exists (default)")
    
    # Example write (to temp location)
    df.write.mode("overwrite").parquet("/tmp/etl_demo/employees/")
    print("\nData written to /tmp/etl_demo/employees/")


def demonstrate_partitioned_write(spark):
    """Demonstrate partitioned writes."""
    print("\n" + "=" * 60)
    print("PARTITIONED WRITES")
    print("=" * 60)
    
    df = spark.createDataFrame([
        (1, "Alice", 2024, 1),
        (2, "Bob", 2024, 1),
        (3, "Charlie", 2024, 2),
        (4, "Diana", 2023, 12)
    ], ["id", "name", "year", "month"])
    
    print("\nSample data:")
    df.show()
    
    # Partitioned write
    df.write.mode("overwrite").partitionBy("year", "month").parquet("/tmp/etl_demo/partitioned/")
    print("\nPartitioned write creates folder structure:")
    print("  /tmp/etl_demo/partitioned/year=2024/month=1/")
    print("  /tmp/etl_demo/partitioned/year=2024/month=2/")
    print("  /tmp/etl_demo/partitioned/year=2023/month=12/")


def main():
    spark = SparkSession.builder.appName("DataLoading").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    
    demonstrate_write_modes(spark)
    demonstrate_partitioned_write(spark)
    
    spark.stop()
    print("\n" + "=" * 60)
    print("ALL DEMONSTRATIONS COMPLETED!")
    print("=" * 60)


if __name__ == "__main__":
    main()
