"""
Error Handling - ETL Learning Hub
=================================
This module demonstrates error handling in ETL using PySpark.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, lit
import logging
import time
from functools import wraps

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger("ETL")


def retry(max_attempts=3, delay=1, backoff=2):
    """Retry decorator with exponential backoff."""
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            attempts = 0
            current_delay = delay
            
            while attempts < max_attempts:
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    attempts += 1
                    if attempts == max_attempts:
                        raise
                    logger.warning(f"Attempt {attempts} failed: {e}")
                    logger.info(f"Retrying in {current_delay} seconds...")
                    time.sleep(current_delay)
                    current_delay *= backoff
        return wrapper
    return decorator


def demonstrate_error_handling(spark):
    """Demonstrate error handling patterns."""
    print("\n" + "=" * 60)
    print("ERROR HANDLING PATTERNS")
    print("=" * 60)
    
    # Sample data
    df = spark.createDataFrame([
        (1, "Alice", 50000),
        (2, "Bob", 60000),
        (3, "Charlie", None)  # Potential error
    ], ["id", "name", "salary"])
    
    print("\nSample data:")
    df.show()
    
    # Safe transformation with error handling
    try:
        result_df = df.withColumn(
            "bonus",
            when(col("salary").isNotNull(), col("salary") * 0.1)
            .otherwise(lit(0))
        )
        print("Transformation with null handling:")
        result_df.show()
        logger.info("Transformation completed successfully")
    except Exception as e:
        logger.error(f"Transformation failed: {e}")
    
    print("\nError Handling Strategies:")
    print("1. Try-catch blocks for each ETL stage")
    print("2. Retry with exponential backoff")
    print("3. Dead letter queue for bad records")
    print("4. Checkpointing for recovery")


def main():
    spark = SparkSession.builder.appName("ErrorHandling").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    
    demonstrate_error_handling(spark)
    
    spark.stop()
    print("\n" + "=" * 60)
    print("ALL DEMONSTRATIONS COMPLETED!")
    print("=" * 60)


if __name__ == "__main__":
    main()
