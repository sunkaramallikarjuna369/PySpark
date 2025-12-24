"""
Spark Actions - PySpark Learning Hub
====================================
This module demonstrates action operations in PySpark.
Actions trigger the execution of transformations and return results.
"""

from pyspark import SparkContext
from pyspark.sql import SparkSession


def demonstrate_basic_actions():
    """Demonstrate basic RDD actions."""
    print("\n" + "=" * 60)
    print("BASIC RDD ACTIONS")
    print("=" * 60)
    
    sc = SparkContext("local[*]", "Basic Actions")
    numbers = sc.parallelize([1, 2, 3, 4, 5, 6, 7, 8, 9, 10])
    
    print(f"\n1. collect(): {numbers.collect()}")
    print(f"2. count(): {numbers.count()}")
    print(f"3. first(): {numbers.first()}")
    print(f"4. take(5): {numbers.take(5)}")
    print(f"5. takeOrdered(3): {numbers.takeOrdered(3)}")
    print(f"6. top(3): {numbers.top(3)}")
    print(f"7. takeSample(3): {numbers.takeSample(False, 3, seed=42)}")
    print(f"8. isEmpty(): {numbers.isEmpty()}")
    
    sc.stop()


def demonstrate_aggregation_actions():
    """Demonstrate aggregation actions."""
    print("\n" + "=" * 60)
    print("AGGREGATION ACTIONS")
    print("=" * 60)
    
    sc = SparkContext("local[*]", "Aggregation Actions")
    numbers = sc.parallelize([1, 2, 3, 4, 5, 6, 7, 8, 9, 10])
    
    print(f"\n1. reduce(sum): {numbers.reduce(lambda a, b: a + b)}")
    print(f"2. fold(0, sum): {numbers.fold(0, lambda a, b: a + b)}")
    
    sum_count = numbers.aggregate(
        (0, 0),
        lambda acc, val: (acc[0] + val, acc[1] + 1),
        lambda acc1, acc2: (acc1[0] + acc2[0], acc1[1] + acc2[1])
    )
    print(f"3. aggregate(sum, count): {sum_count}")
    
    print(f"4. sum(): {numbers.sum()}")
    print(f"5. mean(): {numbers.mean()}")
    print(f"6. max(): {numbers.max()}")
    print(f"7. min(): {numbers.min()}")
    print(f"8. stdev(): {numbers.stdev():.2f}")
    
    data = sc.parallelize([1, 2, 2, 3, 3, 3])
    print(f"9. countByValue(): {dict(data.countByValue())}")
    
    pairs = sc.parallelize([("a", 1), ("b", 2), ("a", 3)])
    print(f"10. countByKey(): {dict(pairs.countByKey())}")
    
    sc.stop()


def demonstrate_dataframe_actions():
    """Demonstrate DataFrame actions."""
    print("\n" + "=" * 60)
    print("DATAFRAME ACTIONS")
    print("=" * 60)
    
    spark = SparkSession.builder.appName("DF Actions").getOrCreate()
    
    data = [
        ("Alice", 25, "Engineering", 50000),
        ("Bob", 30, "Marketing", 60000),
        ("Charlie", 35, "Engineering", 75000)
    ]
    df = spark.createDataFrame(data, ["name", "age", "department", "salary"])
    
    print("\n1. show():")
    df.show()
    
    print(f"2. count(): {df.count()}")
    print(f"3. first(): {df.first()}")
    print(f"4. head(2): {df.head(2)}")
    print(f"5. take(2): {df.take(2)}")
    
    print("\n6. describe():")
    df.describe().show()
    
    print("7. printSchema():")
    df.printSchema()
    
    # Write actions
    df.write.mode("overwrite").csv("/tmp/actions_output", header=True)
    print("\n8. Data written to /tmp/actions_output")
    
    spark.stop()


def main():
    """Main function to run all demonstrations."""
    print("\n" + "=" * 60)
    print("PYSPARK ACTIONS - COMPREHENSIVE GUIDE")
    print("=" * 60)
    
    demonstrate_basic_actions()
    demonstrate_aggregation_actions()
    demonstrate_dataframe_actions()
    
    print("\n" + "=" * 60)
    print("ALL DEMONSTRATIONS COMPLETED!")
    print("=" * 60)


if __name__ == "__main__":
    main()
