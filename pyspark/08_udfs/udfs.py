"""
User Defined Functions (UDFs) - PySpark Learning Hub
=====================================================
This module demonstrates UDF operations in PySpark.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, pandas_udf
from pyspark.sql.types import StringType, FloatType, IntegerType
import pandas as pd


def create_spark_session():
    return SparkSession.builder.appName("UDFs").getOrCreate()


def demonstrate_regular_udfs(spark):
    """Demonstrate regular Python UDFs."""
    print("\n" + "=" * 60)
    print("REGULAR PYTHON UDFs")
    print("=" * 60)
    
    data = [("Alice", "alice@email.com", 75000), ("Bob", "bob@company.org", 80000)]
    df = spark.createDataFrame(data, ["name", "email", "salary"])
    
    @udf(returnType=StringType())
    def extract_domain(email):
        return email.split("@")[1] if email and "@" in email else None
    
    print("\nExtract domain from email:")
    df.withColumn("domain", extract_domain(col("email"))).show()
    
    bonus_udf = udf(lambda s: s * 0.15 if s > 70000 else s * 0.10, FloatType())
    print("Calculate bonus:")
    df.withColumn("bonus", bonus_udf(col("salary"))).show()


def demonstrate_pandas_udfs(spark):
    """Demonstrate Pandas UDFs (vectorized)."""
    print("\n" + "=" * 60)
    print("PANDAS UDFs (VECTORIZED)")
    print("=" * 60)
    
    data = [("Alice", 75000), ("Bob", 80000), ("Charlie", 65000)]
    df = spark.createDataFrame(data, ["name", "salary"])
    
    @pandas_udf(FloatType())
    def calculate_tax(salary: pd.Series) -> pd.Series:
        return salary * 0.25
    
    print("\nCalculate tax with Pandas UDF:")
    df.withColumn("tax", calculate_tax(col("salary"))).show()


def main():
    spark = create_spark_session()
    try:
        demonstrate_regular_udfs(spark)
        demonstrate_pandas_udfs(spark)
        print("\n" + "=" * 60)
        print("ALL DEMONSTRATIONS COMPLETED!")
        print("=" * 60)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
