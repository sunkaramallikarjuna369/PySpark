"""
Accumulators - PySpark Learning Hub
===================================
This module demonstrates accumulators in PySpark.
"""

from pyspark.sql import SparkSession
from pyspark import AccumulatorParam


def create_spark_session():
    return SparkSession.builder.appName("Accumulators").getOrCreate()


def demonstrate_basic_accumulators(spark):
    """Demonstrate basic accumulator usage."""
    print("\n" + "=" * 60)
    print("BASIC ACCUMULATORS")
    print("=" * 60)
    
    sc = spark.sparkContext
    
    counter = sc.accumulator(0)
    sum_acc = sc.accumulator(0)
    
    data = [1, 2, 3, 4, 5]
    rdd = sc.parallelize(data)
    
    def process(x):
        counter.add(1)
        sum_acc.add(x)
        return x * 2
    
    result = rdd.map(process).collect()
    
    print(f"Processed items: {counter.value}")
    print(f"Sum of values: {sum_acc.value}")
    print(f"Result: {result}")


def demonstrate_custom_accumulators(spark):
    """Demonstrate custom accumulators."""
    print("\n" + "=" * 60)
    print("CUSTOM ACCUMULATORS")
    print("=" * 60)
    
    sc = spark.sparkContext
    
    class ListAccumulatorParam(AccumulatorParam):
        def zero(self, initialValue):
            return []
        def addInPlace(self, v1, v2):
            return v1 + v2
    
    list_acc = sc.accumulator([], ListAccumulatorParam())
    
    data = ["apple", "banana", "cherry"]
    rdd = sc.parallelize(data)
    
    rdd.foreach(lambda x: list_acc.add([x]))
    
    print(f"Collected items: {list_acc.value}")


def main():
    spark = create_spark_session()
    try:
        demonstrate_basic_accumulators(spark)
        demonstrate_custom_accumulators(spark)
        print("\n" + "=" * 60)
        print("ALL DEMONSTRATIONS COMPLETED!")
        print("=" * 60)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
