"""
Spark Streaming - PySpark Learning Hub
======================================
This module demonstrates comprehensive Spark Structured Streaming concepts
including basic streaming, windowing, watermarking, stream-to-stream joins,
foreach batch processing, and integration with CSV data.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    window, col, count, avg, sum as spark_sum, max as spark_max,
    current_timestamp, expr, from_json, to_json, struct, lit,
    explode, split, when, date_format
)
from pyspark.sql.types import (
    StructType, StructField, TimestampType, DoubleType, StringType,
    IntegerType, DateType, LongType
)
import os


def create_spark_session():
    """Create SparkSession with streaming configurations."""
    return SparkSession.builder \
        .appName("Spark Streaming Comprehensive") \
        .config("spark.sql.streaming.schemaInference", "true") \
        .config("spark.sql.shuffle.partitions", "4") \
        .getOrCreate()


def demonstrate_file_streaming(spark, data_path="../../data"):
    """Demonstrate file-based streaming with CSV data."""
    print("\n" + "=" * 60)
    print("FILE-BASED STREAMING")
    print("=" * 60)
    
    # Define schema for sales data
    sales_schema = StructType([
        StructField("transaction_id", StringType(), True),
        StructField("date", StringType(), True),
        StructField("customer_id", StringType(), True),
        StructField("product_id", StringType(), True),
        StructField("quantity", IntegerType(), True),
        StructField("unit_price", DoubleType(), True),
        StructField("discount", DoubleType(), True),
        StructField("total_amount", DoubleType(), True),
        StructField("store_id", StringType(), True)
    ])
    
    print("\nSales streaming schema:")
    sales_schema.printTreeString()
    
    # Load static data for demonstration
    if os.path.exists(f"{data_path}/sales.csv"):
        print("\n--- Loading sales data from CSV ---")
        sales_df = spark.read.csv(f"{data_path}/sales.csv", header=True, schema=sales_schema)
        print(f"Loaded {sales_df.count()} sales records")
        sales_df.show(5, truncate=False)
    
    print("\n--- File Streaming Query Example ---")
    print("""
    # Read streaming data from CSV files
    stream = spark.readStream \\
        .schema(sales_schema) \\
        .option("maxFilesPerTrigger", 1) \\
        .option("header", "true") \\
        .csv("/path/to/streaming/sales/")
    
    # Process: Calculate running totals by store
    store_totals = stream \\
        .groupBy("store_id") \\
        .agg(
            spark_sum("total_amount").alias("total_sales"),
            count("*").alias("transaction_count")
        )
    
    # Write to console with complete mode
    query = store_totals.writeStream \\
        .outputMode("complete") \\
        .format("console") \\
        .option("truncate", "false") \\
        .trigger(processingTime="10 seconds") \\
        .start()
    """)


def demonstrate_windowed_streaming(spark, data_path="../../data"):
    """Demonstrate windowed aggregations with real data."""
    print("\n" + "=" * 60)
    print("WINDOWED STREAMING")
    print("=" * 60)
    
    # Load sales data and add timestamp
    if os.path.exists(f"{data_path}/sales.csv"):
        sales_df = spark.read.csv(f"{data_path}/sales.csv", header=True, inferSchema=True)
        sales_with_ts = sales_df.withColumn("event_time", col("date").cast("timestamp"))
        
        print("\n--- Tumbling Window Aggregation ---")
        tumbling = sales_with_ts \
            .groupBy(
                window(col("event_time"), "1 day"),
                col("store_id")
            ) \
            .agg(
                spark_sum("total_amount").alias("daily_sales"),
                count("*").alias("transactions"),
                avg("total_amount").alias("avg_transaction")
            ) \
            .orderBy("window")
        
        print("Daily sales by store (tumbling window):")
        tumbling.show(10, truncate=False)
    
    print("\n--- Tumbling Window Example Code ---")
    print("""
    # Non-overlapping windows of fixed size
    events.groupBy(
        window(col("event_time"), "10 minutes"),  # 10-minute windows
        col("device_id")
    ).agg(
        avg("temperature").alias("avg_temp"),
        spark_max("temperature").alias("max_temp"),
        count("*").alias("reading_count")
    )
    """)
    
    print("\n--- Sliding Window Example Code ---")
    print("""
    # Overlapping windows: 10-minute window, sliding every 5 minutes
    events.groupBy(
        window(col("event_time"), "10 minutes", "5 minutes"),
        col("device_id")
    ).agg(
        avg("temperature").alias("rolling_avg")
    )
    """)
    
    print("\n--- Session Window Example Code (Spark 3.2+) ---")
    print("""
    # Dynamic windows based on activity gaps
    from pyspark.sql.functions import session_window
    
    events.groupBy(
        session_window(col("event_time"), "10 minutes"),  # 10-min gap
        col("user_id")
    ).agg(
        count("*").alias("session_events"),
        spark_sum("value").alias("session_total")
    )
    """)


def demonstrate_watermarking(spark, data_path="../../data"):
    """Demonstrate watermarking for late data handling."""
    print("\n" + "=" * 60)
    print("WATERMARKING FOR LATE DATA")
    print("=" * 60)
    
    print("""
    Watermarking allows Spark to handle late-arriving data by:
    1. Defining a threshold for how late data can arrive
    2. Automatically dropping data that arrives too late
    3. Enabling stateful aggregations with bounded state
    """)
    
    print("\n--- Watermarking Example Code ---")
    print("""
    # Allow data up to 10 minutes late
    watermarked = events \\
        .withWatermark("event_time", "10 minutes") \\
        .groupBy(
            window(col("event_time"), "5 minutes"),
            col("device_id")
        ) \\
        .agg(
            avg("temperature").alias("avg_temp"),
            count("*").alias("count")
        )
    
    # Write with update mode (only changed rows)
    query = watermarked.writeStream \\
        .outputMode("update") \\
        .format("console") \\
        .option("truncate", "false") \\
        .start()
    """)
    
    print("\n--- Watermark Behavior ---")
    print("""
    Event Time: 12:00 | Watermark: 11:50 (10 min threshold)
    
    Incoming events:
    - Event at 12:05 -> ACCEPTED (within watermark)
    - Event at 11:55 -> ACCEPTED (within watermark)
    - Event at 11:45 -> DROPPED (too late, before watermark)
    
    As processing time advances, watermark advances too,
    allowing Spark to clean up old state.
    """)


def demonstrate_stream_joins(spark, data_path="../../data"):
    """Demonstrate stream-to-stream and stream-to-static joins."""
    print("\n" + "=" * 60)
    print("STREAM JOINS")
    print("=" * 60)
    
    # Load static dimension data
    if os.path.exists(f"{data_path}/customers.csv"):
        customers_df = spark.read.csv(f"{data_path}/customers.csv", header=True, inferSchema=True)
        products_df = spark.read.csv(f"{data_path}/products.csv", header=True, inferSchema=True)
        
        print("\n--- Static Dimension Tables ---")
        print(f"Customers: {customers_df.count()} records")
        print(f"Products: {products_df.count()} records")
    
    print("\n--- Stream-to-Static Join Example ---")
    print("""
    # Load static dimension table
    customers = spark.read.csv("customers.csv", header=True)
    
    # Stream of transactions
    transactions = spark.readStream \\
        .schema(transaction_schema) \\
        .csv("/path/to/transactions/")
    
    # Join stream with static data (broadcast small dimension)
    enriched = transactions.join(
        broadcast(customers),
        transactions.customer_id == customers.customer_id,
        "left"
    )
    
    # Write enriched stream
    query = enriched.writeStream \\
        .outputMode("append") \\
        .format("parquet") \\
        .option("path", "/output/enriched/") \\
        .option("checkpointLocation", "/checkpoint/enriched/") \\
        .start()
    """)
    
    print("\n--- Stream-to-Stream Join Example ---")
    print("""
    # Two streams: impressions and clicks
    impressions = spark.readStream.format("kafka") \\
        .option("subscribe", "impressions").load()
    
    clicks = spark.readStream.format("kafka") \\
        .option("subscribe", "clicks").load()
    
    # Join with watermarks (required for stream-stream joins)
    impressions_wm = impressions \\
        .withWatermark("impression_time", "2 hours")
    
    clicks_wm = clicks \\
        .withWatermark("click_time", "3 hours")
    
    # Inner join with time constraint
    joined = impressions_wm.join(
        clicks_wm,
        expr('''
            impressions_wm.ad_id = clicks_wm.ad_id AND
            click_time >= impression_time AND
            click_time <= impression_time + interval 1 hour
        '''),
        "inner"
    )
    """)
    
    print("\n--- Supported Join Types ---")
    print("""
    Stream-Static Joins:
    - Inner, Left Outer, Left Semi supported
    - Static side can be on either side
    
    Stream-Stream Joins:
    - Inner join: Both sides need watermarks
    - Left/Right Outer: Watermark on one side
    - Full Outer: Not supported
    - Time constraints recommended for bounded state
    """)


def demonstrate_foreach_batch(spark, data_path="../../data"):
    """Demonstrate foreachBatch for custom sink processing."""
    print("\n" + "=" * 60)
    print("FOREACH BATCH PROCESSING")
    print("=" * 60)
    
    print("""
    foreachBatch allows you to:
    1. Apply arbitrary operations on each micro-batch
    2. Write to multiple sinks
    3. Use DataFrame writers not available in streaming
    4. Implement exactly-once semantics with idempotent writes
    """)
    
    print("\n--- foreachBatch Example Code ---")
    print("""
    def process_batch(batch_df, batch_id):
        '''Process each micro-batch.'''
        print(f"Processing batch {batch_id} with {batch_df.count()} records")
        
        # Write to multiple destinations
        # 1. Write to data lake (Parquet)
        batch_df.write \\
            .mode("append") \\
            .parquet(f"/data/lake/batch_{batch_id}")
        
        # 2. Write aggregates to database
        aggregates = batch_df.groupBy("category").agg(
            spark_sum("amount").alias("total")
        )
        aggregates.write \\
            .format("jdbc") \\
            .option("url", "jdbc:postgresql://host/db") \\
            .option("dbtable", "aggregates") \\
            .mode("append") \\
            .save()
        
        # 3. Send alerts for high-value transactions
        alerts = batch_df.filter(col("amount") > 10000)
        if alerts.count() > 0:
            send_alerts(alerts.collect())
    
    # Apply foreachBatch
    query = stream.writeStream \\
        .foreachBatch(process_batch) \\
        .option("checkpointLocation", "/checkpoint/") \\
        .start()
    """)
    
    print("\n--- foreach (Row-by-Row) Example ---")
    print("""
    class DatabaseWriter:
        '''Custom writer for each row.'''
        def open(self, partition_id, epoch_id):
            self.connection = create_db_connection()
            return True
        
        def process(self, row):
            self.connection.execute(
                "INSERT INTO events VALUES (?, ?, ?)",
                (row.id, row.value, row.timestamp)
            )
        
        def close(self, error):
            self.connection.close()
    
    query = stream.writeStream \\
        .foreach(DatabaseWriter()) \\
        .start()
    """)


def demonstrate_checkpointing(spark):
    """Demonstrate checkpointing and fault tolerance."""
    print("\n" + "=" * 60)
    print("CHECKPOINTING & FAULT TOLERANCE")
    print("=" * 60)
    
    print("""
    Checkpointing enables:
    1. Fault tolerance - recover from failures
    2. Exactly-once semantics
    3. State management for aggregations
    4. Progress tracking
    """)
    
    print("\n--- Checkpoint Configuration ---")
    print("""
    query = stream.writeStream \\
        .outputMode("update") \\
        .format("parquet") \\
        .option("path", "/output/data/") \\
        .option("checkpointLocation", "/checkpoint/query1/") \\
        .trigger(processingTime="1 minute") \\
        .start()
    
    # Checkpoint directory contains:
    # - commits/     : Completed batch info
    # - offsets/     : Source offsets per batch
    # - state/       : Aggregation state (if any)
    # - metadata     : Query metadata
    """)
    
    print("\n--- Recovery Behavior ---")
    print("""
    On restart:
    1. Spark reads last committed batch from checkpoint
    2. Reprocesses any uncommitted batches
    3. Continues from where it left off
    4. State is restored for stateful operations
    
    Important: Checkpoint location must be on reliable storage
    (HDFS, S3, Azure Blob, etc.) for production.
    """)
    
    print("\n--- State Store Configuration ---")
    print("""
    # Configure state store for large state
    spark.conf.set(
        "spark.sql.streaming.stateStore.providerClass",
        "org.apache.spark.sql.execution.streaming.state.HDFSBackedStateStoreProvider"
    )
    
    # RocksDB state store (Spark 3.2+) for better performance
    spark.conf.set(
        "spark.sql.streaming.stateStore.providerClass",
        "org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider"
    )
    """)


def demonstrate_kafka_streaming(spark):
    """Demonstrate Kafka integration patterns."""
    print("\n" + "=" * 60)
    print("KAFKA STREAMING INTEGRATION")
    print("=" * 60)
    
    print("\n--- Reading from Kafka ---")
    print("""
    # Basic Kafka source
    kafka_df = spark.readStream \\
        .format("kafka") \\
        .option("kafka.bootstrap.servers", "broker1:9092,broker2:9092") \\
        .option("subscribe", "topic1") \\
        .option("startingOffsets", "latest") \\
        .option("failOnDataLoss", "false") \\
        .load()
    
    # Kafka DataFrame columns:
    # - key: binary
    # - value: binary
    # - topic: string
    # - partition: int
    # - offset: long
    # - timestamp: timestamp
    # - timestampType: int
    """)
    
    print("\n--- Parsing Kafka Messages ---")
    print("""
    # Define message schema
    message_schema = StructType([
        StructField("user_id", StringType()),
        StructField("action", StringType()),
        StructField("timestamp", TimestampType()),
        StructField("properties", MapType(StringType(), StringType()))
    ])
    
    # Parse JSON from value
    parsed = kafka_df \\
        .select(
            col("key").cast("string").alias("key"),
            from_json(col("value").cast("string"), message_schema).alias("data"),
            col("timestamp").alias("kafka_timestamp")
        ) \\
        .select("key", "data.*", "kafka_timestamp")
    """)
    
    print("\n--- Writing to Kafka ---")
    print("""
    # Prepare output (must have key and value columns)
    output = processed \\
        .select(
            col("user_id").alias("key"),
            to_json(struct("*")).alias("value")
        )
    
    # Write to Kafka
    query = output.writeStream \\
        .format("kafka") \\
        .option("kafka.bootstrap.servers", "broker1:9092") \\
        .option("topic", "output-topic") \\
        .option("checkpointLocation", "/checkpoint/kafka-output/") \\
        .start()
    """)
    
    print("\n--- Advanced Kafka Options ---")
    print("""
    # Rate limiting
    .option("maxOffsetsPerTrigger", 10000)
    
    # Starting offsets (JSON format)
    .option("startingOffsets", '''
        {"topic1": {"0": 100, "1": 200},
         "topic2": {"0": -2}}
    ''')  # -2 = earliest, -1 = latest
    
    # Kafka consumer configs
    .option("kafka.group.id", "spark-streaming-group")
    .option("kafka.security.protocol", "SASL_SSL")
    .option("kafka.sasl.mechanism", "PLAIN")
    """)


def demonstrate_output_modes(spark):
    """Demonstrate different output modes."""
    print("\n" + "=" * 60)
    print("OUTPUT MODES")
    print("=" * 60)
    
    print("""
    Spark Streaming supports three output modes:
    
    1. APPEND (default for non-aggregations)
       - Only new rows added to result table
       - Cannot be used with aggregations without watermark
       - Best for: Simple transformations, filtering
    
    2. COMPLETE (required for aggregations)
       - Entire result table output every trigger
       - All aggregation results recomputed
       - Best for: Dashboard updates, small result sets
    
    3. UPDATE
       - Only changed rows since last trigger
       - Requires watermark for aggregations
       - Best for: Incremental updates, large result sets
    """)
    
    print("\n--- Output Mode Examples ---")
    print("""
    # APPEND mode - simple filtering
    filtered = stream.filter(col("value") > 100)
    filtered.writeStream.outputMode("append").format("parquet").start()
    
    # COMPLETE mode - aggregations
    counts = stream.groupBy("category").count()
    counts.writeStream.outputMode("complete").format("console").start()
    
    # UPDATE mode - with watermark
    windowed = stream \\
        .withWatermark("timestamp", "10 minutes") \\
        .groupBy(window("timestamp", "5 minutes")) \\
        .count()
    windowed.writeStream.outputMode("update").format("console").start()
    """)


def demonstrate_triggers(spark):
    """Demonstrate different trigger types."""
    print("\n" + "=" * 60)
    print("TRIGGER TYPES")
    print("=" * 60)
    
    print("""
    Triggers control when streaming queries process data:
    
    1. Default (micro-batch as fast as possible)
       .trigger()  # or no trigger specified
    
    2. Fixed interval micro-batch
       .trigger(processingTime="10 seconds")
       .trigger(processingTime="1 minute")
    
    3. One-time micro-batch (Spark 2.3+)
       .trigger(once=True)
       # Processes all available data, then stops
       # Great for: Backfilling, testing, scheduled jobs
    
    4. Available-now micro-batch (Spark 3.3+)
       .trigger(availableNow=True)
       # Like once=True but processes in multiple batches
       # Better for: Large backfills with rate limiting
    
    5. Continuous processing (Spark 2.3+, experimental)
       .trigger(continuous="1 second")
       # Sub-millisecond latency
       # Limited operations supported
    """)
    
    print("\n--- Trigger Examples ---")
    print("""
    # Process every 30 seconds
    query = stream.writeStream \\
        .trigger(processingTime="30 seconds") \\
        .format("parquet") \\
        .start()
    
    # One-time batch processing
    query = stream.writeStream \\
        .trigger(once=True) \\
        .format("parquet") \\
        .start()
    query.awaitTermination()  # Wait for completion
    
    # Continuous processing (low latency)
    query = stream.writeStream \\
        .trigger(continuous="100 milliseconds") \\
        .format("kafka") \\
        .start()
    """)


def demonstrate_monitoring(spark):
    """Demonstrate streaming query monitoring."""
    print("\n" + "=" * 60)
    print("MONITORING STREAMING QUERIES")
    print("=" * 60)
    
    print("""
    Monitoring streaming queries is essential for production:
    
    1. Query Progress
       query.lastProgress  # Last micro-batch stats
       query.recentProgress  # Recent batches
    
    2. Query Status
       query.status  # Current status
       query.isActive  # Is query running?
       query.exception  # Any exception
    
    3. StreamingQueryListener
       Custom listener for metrics/alerts
    """)
    
    print("\n--- Query Progress Example ---")
    print("""
    progress = query.lastProgress
    print(f"Batch ID: {progress['batchId']}")
    print(f"Input rows: {progress['numInputRows']}")
    print(f"Processing rate: {progress['processedRowsPerSecond']}")
    print(f"Batch duration: {progress['batchDuration']} ms")
    
    # Source-specific progress
    for source in progress['sources']:
        print(f"Source: {source['description']}")
        print(f"  Start offset: {source['startOffset']}")
        print(f"  End offset: {source['endOffset']}")
    """)
    
    print("\n--- Custom StreamingQueryListener ---")
    print("""
    from pyspark.sql.streaming import StreamingQueryListener
    
    class MetricsListener(StreamingQueryListener):
        def onQueryStarted(self, event):
            print(f"Query started: {event.id}")
        
        def onQueryProgress(self, event):
            progress = event.progress
            # Send metrics to monitoring system
            send_metrics({
                'query_id': str(progress.id),
                'batch_id': progress.batchId,
                'input_rows': progress.numInputRows,
                'processing_rate': progress.processedRowsPerSecond
            })
        
        def onQueryTerminated(self, event):
            print(f"Query terminated: {event.id}")
            if event.exception:
                send_alert(f"Query failed: {event.exception}")
    
    # Register listener
    spark.streams.addListener(MetricsListener())
    """)


def main():
    """Main function demonstrating all streaming concepts."""
    spark = create_spark_session()
    data_path = "../../data"
    
    try:
        print("\n" + "=" * 70)
        print("SPARK STRUCTURED STREAMING - COMPREHENSIVE GUIDE")
        print("=" * 70)
        
        demonstrate_file_streaming(spark, data_path)
        demonstrate_windowed_streaming(spark, data_path)
        demonstrate_watermarking(spark, data_path)
        demonstrate_stream_joins(spark, data_path)
        demonstrate_foreach_batch(spark, data_path)
        demonstrate_checkpointing(spark)
        demonstrate_kafka_streaming(spark)
        demonstrate_output_modes(spark)
        demonstrate_triggers(spark)
        demonstrate_monitoring(spark)
        
        print("\n" + "=" * 70)
        print("ALL STREAMING DEMONSTRATIONS COMPLETED!")
        print("=" * 70)
        print("""
        Key Takeaways:
        1. Structured Streaming treats streams as unbounded tables
        2. Use watermarks for late data and bounded state
        3. Choose output mode based on your use case
        4. Always use checkpointing in production
        5. Monitor queries with progress and listeners
        6. foreachBatch enables custom sink logic
        """)
        
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
