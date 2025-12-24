"""
Logging & Monitoring - ETL Learning Hub
=======================================
This module demonstrates logging and monitoring in ETL using PySpark.
"""

from pyspark.sql import SparkSession
import logging
from datetime import datetime
import json


class JSONFormatter(logging.Formatter):
    """JSON formatter for structured logging."""
    def format(self, record):
        log_obj = {
            "timestamp": datetime.utcnow().isoformat(),
            "level": record.levelname,
            "message": record.getMessage(),
            "module": record.module
        }
        return json.dumps(log_obj)


# Setup logger
logger = logging.getLogger("ETL")
handler = logging.StreamHandler()
handler.setFormatter(JSONFormatter())
logger.addHandler(handler)
logger.setLevel(logging.INFO)


def demonstrate_logging(spark):
    """Demonstrate ETL logging."""
    print("\n" + "=" * 60)
    print("ETL LOGGING")
    print("=" * 60)
    
    job_id = f"etl_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    
    logger.info(f"ETL job started: {job_id}")
    
    # Sample ETL operation
    df = spark.createDataFrame([
        (1, "Alice", 50000),
        (2, "Bob", 60000),
        (3, "Charlie", 75000)
    ], ["id", "name", "salary"])
    
    record_count = df.count()
    logger.info(f"Extracted {record_count} records")
    
    # Transform
    transformed_df = df.filter(df.salary > 55000)
    logger.info(f"Transformed: {transformed_df.count()} records after filter")
    
    logger.info(f"ETL job completed: {job_id}")
    
    print("\nLog output (JSON format):")
    print("See console output above for structured logs")


def demonstrate_metrics(spark):
    """Demonstrate metrics collection."""
    print("\n" + "=" * 60)
    print("ETL METRICS")
    print("=" * 60)
    
    print("\nKey ETL Metrics to Track:")
    print("1. Records read/written")
    print("2. Processing duration")
    print("3. Error/failure count")
    print("4. Throughput (records/second)")
    print("5. Data quality scores")
    print("6. Resource utilization")


def main():
    spark = SparkSession.builder.appName("LoggingMonitoring").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    
    demonstrate_logging(spark)
    demonstrate_metrics(spark)
    
    spark.stop()
    print("\n" + "=" * 60)
    print("ALL DEMONSTRATIONS COMPLETED!")
    print("=" * 60)


if __name__ == "__main__":
    main()
