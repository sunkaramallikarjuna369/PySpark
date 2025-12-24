"""
Caching & Persistence - PySpark Learning Hub
=============================================
This module demonstrates caching and persistence in PySpark.
"""

from pyspark.sql import SparkSession
from pyspark import StorageLevel
from pyspark.sql.functions import col
import time


def create_spark_session():
    return SparkSession.builder.appName("Caching & Persistence").getOrCreate()


def demonstrate_caching(spark):
    """Demonstrate basic caching."""
    print("\n" + "=" * 60)
    print("BASIC CACHING")
    print("=" * 60)
    
    df = spark.range(1000000).withColumn("value", col("id") * 2)
    
    print("\nWithout caching:")
    start = time.time()
    df.count()
    print(f"First count: {time.time() - start:.3f}s")
    
    start = time.time()
    df.count()
    print(f"Second count: {time.time() - start:.3f}s")
    
    print("\nWith caching:")
    df_cached = df.cache()
    
    start = time.time()
    df_cached.count()
    print(f"First count (caching): {time.time() - start:.3f}s")
    
    start = time.time()
    df_cached.count()
    print(f"Second count (cached): {time.time() - start:.3f}s")
    
    df_cached.unpersist()


def demonstrate_storage_levels(spark):
    """Demonstrate different storage levels."""
    print("\n" + "=" * 60)
    print("STORAGE LEVELS")
    print("=" * 60)
    
    df = spark.range(100000)
    
    levels = [
        ("MEMORY_ONLY", StorageLevel.MEMORY_ONLY),
        ("MEMORY_AND_DISK", StorageLevel.MEMORY_AND_DISK),
        ("MEMORY_ONLY_SER", StorageLevel.MEMORY_ONLY_SER),
        ("DISK_ONLY", StorageLevel.DISK_ONLY)
    ]
    
    for name, level in levels:
        df_persisted = df.persist(level)
        df_persisted.count()
        print(f"{name}: {df_persisted.storageLevel}")
        df_persisted.unpersist()


def main():
    spark = create_spark_session()
    try:
        demonstrate_caching(spark)
        demonstrate_storage_levels(spark)
        print("\n" + "=" * 60)
        print("ALL DEMONSTRATIONS COMPLETED!")
        print("=" * 60)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
