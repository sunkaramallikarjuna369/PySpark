"""
Data Transformation - ETL Learning Hub
======================================
This module demonstrates data transformation techniques using PySpark.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, upper, lower, when, coalesce, lit, current_timestamp


def demonstrate_data_cleaning(spark):
    """Demonstrate data cleaning transformations."""
    print("\n" + "=" * 60)
    print("DATA CLEANING")
    print("=" * 60)
    
    raw_df = spark.createDataFrame([
        (1, "  Alice  ", "ALICE@email.com", 50000),
        (2, "BOB", "bob@email.com", None),
        (3, "charlie", None, 60000)
    ], ["id", "name", "email", "salary"])
    
    print("\nRaw data:")
    raw_df.show()
    
    cleaned_df = raw_df \
        .withColumn("name", upper(trim(col("name")))) \
        .withColumn("email", lower(col("email"))) \
        .withColumn("salary", coalesce(col("salary"), lit(0)))
    
    print("Cleaned data:")
    cleaned_df.show()


def demonstrate_enrichment(spark):
    """Demonstrate data enrichment."""
    print("\n" + "=" * 60)
    print("DATA ENRICHMENT")
    print("=" * 60)
    
    df = spark.createDataFrame([
        (1, "Alice", 50000),
        (2, "Bob", 80000),
        (3, "Charlie", 120000)
    ], ["id", "name", "salary"])
    
    enriched_df = df \
        .withColumn("salary_band",
            when(col("salary") >= 100000, "Executive")
            .when(col("salary") >= 70000, "Senior")
            .otherwise("Junior")) \
        .withColumn("processed_at", current_timestamp())
    
    print("Enriched data:")
    enriched_df.show(truncate=False)


def main():
    spark = SparkSession.builder.appName("DataTransformation").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    
    demonstrate_data_cleaning(spark)
    demonstrate_enrichment(spark)
    
    spark.stop()
    print("\n" + "=" * 60)
    print("ALL DEMONSTRATIONS COMPLETED!")
    print("=" * 60)


if __name__ == "__main__":
    main()
