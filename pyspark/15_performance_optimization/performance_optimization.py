"""
Performance Optimization - PySpark Learning Hub
===============================================
This module demonstrates performance optimization techniques in PySpark.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, broadcast, when, lit, concat, rand


def create_optimized_spark_session():
    """Create an optimized Spark session."""
    return SparkSession.builder \
        .appName("Performance Optimization") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.sql.adaptive.skewJoin.enabled", "true") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .getOrCreate()


def demonstrate_skew_handling(spark):
    """Demonstrate data skew handling techniques."""
    print("\n" + "=" * 60)
    print("DATA SKEW HANDLING")
    print("=" * 60)
    
    skewed_df = spark.range(100000).withColumn(
        "key",
        when(col("id") < 90000, lit("hot_key"))
        .otherwise(concat(lit("key_"), col("id")))
    )
    
    print("\nSkew distribution:")
    skewed_df.groupBy("key").count().orderBy(col("count").desc()).show(5)
    
    print("\nSolution 1: Salting")
    num_salts = 10
    salted = skewed_df.withColumn(
        "salted_key",
        concat(col("key"), lit("_"), (rand() * num_salts).cast("int"))
    )
    print("Salted key distribution:")
    salted.groupBy("salted_key").count().orderBy(col("count").desc()).show(5)


def demonstrate_optimization_tips(spark):
    """Demonstrate general optimization tips."""
    print("\n" + "=" * 60)
    print("OPTIMIZATION TIPS")
    print("=" * 60)
    
    df = spark.range(1000000)
    
    print("\n1. Filter early:")
    print("   df.filter(col('id') < 1000).groupBy(...)")
    
    print("\n2. Use broadcast for small tables:")
    print("   large_df.join(broadcast(small_df), 'key')")
    
    print("\n3. Coalesce vs Repartition:")
    print("   - coalesce(n): Reduce partitions without shuffle")
    print("   - repartition(n): Full shuffle, even distribution")
    
    print("\n4. Cache strategically:")
    print("   expensive_df.cache()")
    print("   expensive_df.count()  # Trigger caching")
    
    print("\n5. Avoid UDFs when possible:")
    print("   Use built-in functions instead")


def main():
    spark = create_optimized_spark_session()
    try:
        demonstrate_skew_handling(spark)
        demonstrate_optimization_tips(spark)
        print("\n" + "=" * 60)
        print("ALL DEMONSTRATIONS COMPLETED!")
        print("=" * 60)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
