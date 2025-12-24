"""
ETL Best Practices - ETL Learning Hub
=====================================
This module demonstrates ETL best practices using PySpark.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, current_timestamp


def demonstrate_idempotency(spark):
    """Demonstrate idempotent ETL patterns."""
    print("\n" + "=" * 60)
    print("IDEMPOTENCY")
    print("=" * 60)
    
    print("\nIdempotent ETL: Running multiple times produces same result")
    print("\nPatterns:")
    print("1. Use MERGE/UPSERT instead of INSERT")
    print("2. Use overwrite mode with full refresh")
    print("3. Deduplicate before loading")
    print("4. Use unique job run IDs")


def demonstrate_modularity(spark):
    """Demonstrate modular ETL design."""
    print("\n" + "=" * 60)
    print("MODULARITY")
    print("=" * 60)
    
    # Sample modular transformations
    def clean_data(df):
        return df.na.drop()
    
    def standardize_names(df):
        return df.withColumn("name", col("name").cast("string"))
    
    def add_metadata(df):
        return df.withColumn("processed_at", current_timestamp())
    
    # Create sample data
    df = spark.createDataFrame([
        (1, "Alice", 50000),
        (2, "Bob", 60000),
        (3, None, 75000)
    ], ["id", "name", "salary"])
    
    print("\nOriginal data:")
    df.show()
    
    # Apply modular transformations
    result = df
    for transform in [clean_data, standardize_names, add_metadata]:
        result = transform(result)
    
    print("After modular transformations:")
    result.show(truncate=False)


def demonstrate_best_practices(spark):
    """Demonstrate key best practices."""
    print("\n" + "=" * 60)
    print("ETL BEST PRACTICES SUMMARY")
    print("=" * 60)
    
    print("\nDesign Principles:")
    print("1. Idempotency - Same result on re-run")
    print("2. Modularity - Reusable components")
    print("3. Testability - Unit and integration tests")
    print("4. Documentation - Clear naming and comments")
    
    print("\nPerformance:")
    print("1. Partition pruning - Filter early")
    print("2. Column pruning - Select only needed columns")
    print("3. Broadcast small tables")
    print("4. Avoid unnecessary shuffles")
    print("5. Cache strategically")
    
    print("\nOperations:")
    print("1. Configuration management")
    print("2. Environment separation")
    print("3. Monitoring and alerting")
    print("4. Data validation gates")
    print("5. Rollback capability")


def main():
    spark = SparkSession.builder.appName("ETLBestPractices").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    
    demonstrate_idempotency(spark)
    demonstrate_modularity(spark)
    demonstrate_best_practices(spark)
    
    spark.stop()
    print("\n" + "=" * 60)
    print("ALL DEMONSTRATIONS COMPLETED!")
    print("=" * 60)


if __name__ == "__main__":
    main()
