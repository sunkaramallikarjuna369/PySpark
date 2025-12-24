"""
Spark Transformations - PySpark Learning Hub
=============================================
This module demonstrates transformation operations in PySpark.
Transformations are lazy operations that create new RDDs/DataFrames.
"""

from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr, when, lit, upper, lower
from pyspark.sql.functions import count, avg, sum as spark_sum


def demonstrate_narrow_transformations():
    """Demonstrate narrow transformations (no shuffle)."""
    print("\n" + "=" * 60)
    print("NARROW TRANSFORMATIONS")
    print("=" * 60)
    
    sc = SparkContext("local[*]", "Narrow Transformations")
    
    numbers = sc.parallelize([1, 2, 3, 4, 5, 6, 7, 8, 9, 10])
    words = sc.parallelize(["hello world", "spark transformations"])
    
    # map()
    squared = numbers.map(lambda x: x ** 2)
    print(f"\n1. map(x^2): {squared.collect()}")
    
    # filter()
    evens = numbers.filter(lambda x: x % 2 == 0)
    print(f"2. filter(even): {evens.collect()}")
    
    # flatMap()
    word_list = words.flatMap(lambda line: line.split(" "))
    print(f"3. flatMap(split): {word_list.collect()}")
    
    # mapPartitions()
    def sum_partition(iterator):
        yield sum(iterator)
    partition_sums = numbers.mapPartitions(sum_partition)
    print(f"4. mapPartitions(sum): {partition_sums.collect()}")
    
    # distinct()
    with_dups = sc.parallelize([1, 2, 2, 3, 3, 3])
    unique = with_dups.distinct()
    print(f"5. distinct(): {unique.collect()}")
    
    # union()
    rdd1 = sc.parallelize([1, 2, 3])
    rdd2 = sc.parallelize([4, 5, 6])
    combined = rdd1.union(rdd2)
    print(f"6. union(): {combined.collect()}")
    
    # intersection()
    rdd3 = sc.parallelize([1, 2, 3, 4])
    rdd4 = sc.parallelize([3, 4, 5, 6])
    common = rdd3.intersection(rdd4)
    print(f"7. intersection(): {common.collect()}")
    
    # subtract()
    diff = rdd3.subtract(rdd4)
    print(f"8. subtract(): {diff.collect()}")
    
    sc.stop()


def demonstrate_wide_transformations():
    """Demonstrate wide transformations (with shuffle)."""
    print("\n" + "=" * 60)
    print("WIDE TRANSFORMATIONS")
    print("=" * 60)
    
    sc = SparkContext("local[*]", "Wide Transformations")
    
    pairs = sc.parallelize([
        ("apple", 3), ("banana", 2), ("apple", 5),
        ("orange", 1), ("banana", 4)
    ])
    
    # groupByKey()
    grouped = pairs.groupByKey()
    result = [(k, list(v)) for k, v in grouped.collect()]
    print(f"\n1. groupByKey(): {result}")
    
    # reduceByKey()
    sum_by_key = pairs.reduceByKey(lambda a, b: a + b)
    print(f"2. reduceByKey(sum): {sum_by_key.collect()}")
    
    # sortByKey()
    sorted_pairs = pairs.sortByKey()
    print(f"3. sortByKey(): {sorted_pairs.collect()}")
    
    # Join operations
    employees = sc.parallelize([(1, "Alice"), (2, "Bob"), (3, "Charlie")])
    salaries = sc.parallelize([(1, 50000), (2, 60000), (4, 70000)])
    
    inner = employees.join(salaries)
    print(f"4. join(): {inner.collect()}")
    
    left = employees.leftOuterJoin(salaries)
    print(f"5. leftOuterJoin(): {left.collect()}")
    
    # repartition()
    data = sc.parallelize(range(100), 4)
    print(f"6. Original partitions: {data.getNumPartitions()}")
    repartitioned = data.repartition(8)
    print(f"   After repartition(8): {repartitioned.getNumPartitions()}")
    
    # coalesce()
    coalesced = repartitioned.coalesce(2)
    print(f"7. After coalesce(2): {coalesced.getNumPartitions()}")
    
    sc.stop()


def demonstrate_dataframe_transformations():
    """Demonstrate DataFrame transformations."""
    print("\n" + "=" * 60)
    print("DATAFRAME TRANSFORMATIONS")
    print("=" * 60)
    
    spark = SparkSession.builder.appName("DF Transformations").getOrCreate()
    
    data = [
        ("Alice", 25, "Engineering", 50000),
        ("Bob", 30, "Marketing", 60000),
        ("Charlie", 35, "Engineering", 75000),
        ("Diana", 28, "Sales", 55000)
    ]
    df = spark.createDataFrame(data, ["name", "age", "department", "salary"])
    
    # select()
    print("\n1. select():")
    df.select("name", "salary").show()
    
    # withColumn()
    print("2. withColumn():")
    df.withColumn("bonus", col("salary") * 0.1).show()
    
    # filter()
    print("3. filter():")
    df.filter(col("age") > 28).show()
    
    # orderBy()
    print("4. orderBy():")
    df.orderBy(col("salary").desc()).show()
    
    # groupBy()
    print("5. groupBy():")
    df.groupBy("department").agg(
        count("*").alias("count"),
        avg("salary").alias("avg_salary")
    ).show()
    
    # Chained transformations
    print("6. Chained transformations:")
    result = (df
        .filter(col("age") > 25)
        .withColumn("annual_bonus", col("salary") * 0.15)
        .select("name", "department", "annual_bonus")
        .orderBy(col("annual_bonus").desc())
    )
    result.show()
    
    spark.stop()


def main():
    """Main function to run all demonstrations."""
    print("\n" + "=" * 60)
    print("PYSPARK TRANSFORMATIONS - COMPREHENSIVE GUIDE")
    print("=" * 60)
    
    demonstrate_narrow_transformations()
    demonstrate_wide_transformations()
    demonstrate_dataframe_transformations()
    
    print("\n" + "=" * 60)
    print("ALL DEMONSTRATIONS COMPLETED!")
    print("=" * 60)


if __name__ == "__main__":
    main()
