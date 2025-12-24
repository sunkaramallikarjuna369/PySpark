"""
Spark Streaming - PySpark Learning Hub
======================================
This module demonstrates Spark Structured Streaming.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import window, col, count, avg
from pyspark.sql.types import StructType, TimestampType, DoubleType, StringType


def create_spark_session():
    return SparkSession.builder.appName("Spark Streaming").getOrCreate()


def demonstrate_file_streaming(spark):
    """Demonstrate file-based streaming."""
    print("\n" + "=" * 60)
    print("FILE-BASED STREAMING")
    print("=" * 60)
    
    schema = StructType() \
        .add("id", "integer") \
        .add("name", "string") \
        .add("value", "double") \
        .add("timestamp", "timestamp")
    
    print("\nFile streaming schema defined:")
    print(schema)
    
    print("\nExample streaming query:")
    print("""
    stream = spark.readStream \\
        .schema(schema) \\
        .option("maxFilesPerTrigger", 1) \\
        .csv("/path/to/data")
    
    query = stream.writeStream \\
        .outputMode("append") \\
        .format("console") \\
        .start()
    """)


def demonstrate_windowed_streaming(spark):
    """Demonstrate windowed aggregations."""
    print("\n" + "=" * 60)
    print("WINDOWED STREAMING")
    print("=" * 60)
    
    print("\nTumbling Window Example:")
    print("""
    events.groupBy(
        window(col("event_time"), "10 minutes"),
        col("device_id")
    ).agg(avg("temperature"))
    """)
    
    print("\nSliding Window Example:")
    print("""
    events.groupBy(
        window(col("event_time"), "10 minutes", "5 minutes"),
        col("device_id")
    ).agg(avg("temperature"))
    """)
    
    print("\nWatermarking Example:")
    print("""
    events.withWatermark("event_time", "10 minutes")
        .groupBy(window(col("event_time"), "5 minutes"))
        .count()
    """)


def main():
    spark = create_spark_session()
    try:
        demonstrate_file_streaming(spark)
        demonstrate_windowed_streaming(spark)
        print("\n" + "=" * 60)
        print("ALL DEMONSTRATIONS COMPLETED!")
        print("=" * 60)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
