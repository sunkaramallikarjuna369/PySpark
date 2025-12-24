"""
Data Quality - ETL Learning Hub
===============================
This module demonstrates data quality checks using PySpark.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, when, isnan, isnull, sum as spark_sum


def check_completeness(df, columns):
    """Check for null/missing values."""
    total_rows = df.count()
    results = {}
    
    for column in columns:
        null_count = df.filter(col(column).isNull()).count()
        completeness = (total_rows - null_count) / total_rows * 100
        results[column] = {
            "null_count": null_count,
            "completeness_pct": round(completeness, 2)
        }
    return results


def check_uniqueness(df, key_columns):
    """Check for duplicate records."""
    total = df.count()
    distinct = df.dropDuplicates(key_columns).count()
    duplicates = total - distinct
    return {
        "total_rows": total,
        "unique_rows": distinct,
        "duplicate_rows": duplicates,
        "uniqueness_pct": round(distinct / total * 100, 2)
    }


def demonstrate_data_quality(spark):
    """Demonstrate data quality checks."""
    print("\n" + "=" * 60)
    print("DATA QUALITY CHECKS")
    print("=" * 60)
    
    # Sample data with quality issues
    df = spark.createDataFrame([
        (1, "Alice", "alice@email.com", 50000),
        (2, "Bob", None, 60000),
        (3, "Charlie", "charlie@email.com", None),
        (1, "Alice", "alice@email.com", 50000),  # Duplicate
        (4, "Diana", "invalid-email", 55000)
    ], ["id", "name", "email", "salary"])
    
    print("\nSample data with quality issues:")
    df.show()
    
    # Completeness check
    print("\nCompleteness Check:")
    completeness = check_completeness(df, ["email", "salary"])
    for col_name, result in completeness.items():
        print(f"  {col_name}: {result['completeness_pct']}% complete ({result['null_count']} nulls)")
    
    # Uniqueness check
    print("\nUniqueness Check:")
    uniqueness = check_uniqueness(df, ["id"])
    print(f"  Total rows: {uniqueness['total_rows']}")
    print(f"  Unique rows: {uniqueness['unique_rows']}")
    print(f"  Duplicates: {uniqueness['duplicate_rows']}")


def main():
    spark = SparkSession.builder.appName("DataQuality").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    
    demonstrate_data_quality(spark)
    
    spark.stop()
    print("\n" + "=" * 60)
    print("ALL DEMONSTRATIONS COMPLETED!")
    print("=" * 60)


if __name__ == "__main__":
    main()
