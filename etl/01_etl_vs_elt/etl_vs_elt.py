"""
ETL vs ELT - ETL Learning Hub
=============================
This module demonstrates ETL and ELT approaches using PySpark.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, upper, when


def demonstrate_etl(spark):
    """Demonstrate ETL approach."""
    print("\n" + "=" * 60)
    print("ETL APPROACH")
    print("=" * 60)
    
    # Create sample data
    data = [
        (1, "alice", 1500, "active"),
        (2, "bob", 500, "active"),
        (3, "charlie", 50, "inactive"),
        (4, "diana", 2000, "active")
    ]
    raw_df = spark.createDataFrame(data, ["id", "name", "amount", "status"])
    
    print("\n1. EXTRACT - Raw data:")
    raw_df.show()
    
    # 2. TRANSFORM
    transformed_df = (raw_df
        .filter(col("status") == "active")
        .withColumn("name", upper(col("name")))
        .withColumn("category", 
            when(col("amount") > 1000, "high")
            .when(col("amount") > 100, "medium")
            .otherwise("low"))
        .select("id", "name", "amount", "category")
    )
    
    print("2. TRANSFORM - Cleaned and enriched:")
    transformed_df.show()
    
    print("3. LOAD - Write transformed data to warehouse")
    print("   (In production: transformed_df.write.parquet('warehouse/'))")


def demonstrate_elt(spark):
    """Demonstrate ELT approach."""
    print("\n" + "=" * 60)
    print("ELT APPROACH")
    print("=" * 60)
    
    data = [
        (1, "alice", 1500, "active"),
        (2, "bob", 500, "active"),
        (3, "charlie", 50, "inactive"),
        (4, "diana", 2000, "active")
    ]
    raw_df = spark.createDataFrame(data, ["id", "name", "amount", "status"])
    
    print("\n1. EXTRACT - Raw data:")
    raw_df.show()
    
    print("2. LOAD - Write raw data to data lake")
    print("   (In production: raw_df.write.parquet('data_lake/raw/'))")
    
    # Simulate loading to data lake
    raw_df.createOrReplaceTempView("raw_data")
    
    # 3. TRANSFORM in warehouse
    transformed_df = spark.sql("""
        SELECT 
            id,
            UPPER(name) as name,
            amount,
            CASE 
                WHEN amount > 1000 THEN 'high'
                WHEN amount > 100 THEN 'medium'
                ELSE 'low'
            END as category
        FROM raw_data
        WHERE status = 'active'
    """)
    
    print("3. TRANSFORM - In warehouse using SQL:")
    transformed_df.show()


def main():
    spark = SparkSession.builder.appName("ETL_vs_ELT").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    
    demonstrate_etl(spark)
    demonstrate_elt(spark)
    
    spark.stop()
    print("\n" + "=" * 60)
    print("ALL DEMONSTRATIONS COMPLETED!")
    print("=" * 60)


if __name__ == "__main__":
    main()
