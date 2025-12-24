"""
RDD Basics - PySpark Learning Hub
=================================
This module demonstrates the fundamental concepts of Resilient Distributed Datasets (RDDs)
in Apache Spark using PySpark.

RDDs are the core abstraction in Spark - immutable, distributed collections of objects
that can be processed in parallel across a cluster.
"""

from pyspark import SparkContext, SparkConf, StorageLevel
from typing import List, Tuple, Any


def create_spark_context(app_name: str = "RDD Basics") -> SparkContext:
    """Create and return a SparkContext."""
    conf = SparkConf().setAppName(app_name).setMaster("local[*]")
    return SparkContext(conf=conf)


def demonstrate_rdd_creation(sc: SparkContext) -> None:
    """Demonstrate various ways to create RDDs."""
    print("\n" + "=" * 60)
    print("RDD CREATION METHODS")
    print("=" * 60)
    
    # Method 1: From a Python collection using parallelize()
    data = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
    rdd_from_list = sc.parallelize(data)
    print(f"\n1. RDD from list: {rdd_from_list.collect()}")
    
    # Method 2: With specific number of partitions
    rdd_partitioned = sc.parallelize(data, numSlices=4)
    print(f"2. RDD with 4 partitions: {rdd_partitioned.getNumPartitions()} partitions")
    
    # Method 3: From a range
    range_rdd = sc.parallelize(range(0, 20, 2))
    print(f"3. RDD from range: {range_rdd.collect()}")
    
    # Method 4: Empty RDD
    empty_rdd = sc.emptyRDD()
    print(f"4. Empty RDD count: {empty_rdd.count()}")
    
    # Method 5: From tuples (key-value pairs)
    pairs = [("a", 1), ("b", 2), ("c", 3), ("a", 4)]
    pair_rdd = sc.parallelize(pairs)
    print(f"5. Pair RDD: {pair_rdd.collect()}")


def demonstrate_transformations(sc: SparkContext) -> None:
    """Demonstrate common RDD transformations."""
    print("\n" + "=" * 60)
    print("RDD TRANSFORMATIONS (Lazy Operations)")
    print("=" * 60)
    
    numbers = sc.parallelize([1, 2, 3, 4, 5, 6, 7, 8, 9, 10])
    
    # map() - Apply function to each element
    squared = numbers.map(lambda x: x ** 2)
    print(f"\n1. map(x => x^2): {squared.collect()}")
    
    # filter() - Keep elements matching condition
    evens = numbers.filter(lambda x: x % 2 == 0)
    print(f"2. filter(even): {evens.collect()}")
    
    # flatMap() - Map and flatten
    words = sc.parallelize(["hello world", "spark rdd"])
    word_list = words.flatMap(lambda line: line.split(" "))
    print(f"3. flatMap(split): {word_list.collect()}")
    
    # distinct() - Remove duplicates
    with_dups = sc.parallelize([1, 2, 2, 3, 3, 3, 4])
    unique = with_dups.distinct()
    print(f"4. distinct(): {unique.collect()}")
    
    # union() - Combine RDDs
    rdd1 = sc.parallelize([1, 2, 3])
    rdd2 = sc.parallelize([4, 5, 6])
    combined = rdd1.union(rdd2)
    print(f"5. union(): {combined.collect()}")
    
    # intersection() - Common elements
    rdd3 = sc.parallelize([1, 2, 3, 4])
    rdd4 = sc.parallelize([3, 4, 5, 6])
    common = rdd3.intersection(rdd4)
    print(f"6. intersection(): {common.collect()}")
    
    # subtract() - Elements in first but not second
    diff = rdd3.subtract(rdd4)
    print(f"7. subtract(): {diff.collect()}")
    
    # cartesian() - Cartesian product
    cart = sc.parallelize([1, 2]).cartesian(sc.parallelize(['a', 'b']))
    print(f"8. cartesian(): {cart.collect()}")
    
    # sortBy() - Sort elements
    unsorted = sc.parallelize([5, 2, 8, 1, 9, 3])
    sorted_rdd = unsorted.sortBy(lambda x: x)
    print(f"9. sortBy(): {sorted_rdd.collect()}")
    
    # groupBy() - Group by key function
    grouped = numbers.groupBy(lambda x: "even" if x % 2 == 0 else "odd")
    result = [(k, list(v)) for k, v in grouped.collect()]
    print(f"10. groupBy(): {result}")
    
    # mapPartitions() - Apply function to each partition
    def sum_partition(iterator):
        yield sum(iterator)
    
    partition_sums = numbers.mapPartitions(sum_partition)
    print(f"11. mapPartitions(sum): {partition_sums.collect()}")
    
    # sample() - Random sample
    sampled = numbers.sample(withReplacement=False, fraction=0.5, seed=42)
    print(f"12. sample(50%): {sampled.collect()}")


def demonstrate_actions(sc: SparkContext) -> None:
    """Demonstrate common RDD actions."""
    print("\n" + "=" * 60)
    print("RDD ACTIONS (Trigger Computation)")
    print("=" * 60)
    
    numbers = sc.parallelize([1, 2, 3, 4, 5, 6, 7, 8, 9, 10])
    
    # collect() - Return all elements
    print(f"\n1. collect(): {numbers.collect()}")
    
    # count() - Count elements
    print(f"2. count(): {numbers.count()}")
    
    # first() - First element
    print(f"3. first(): {numbers.first()}")
    
    # take(n) - First n elements
    print(f"4. take(5): {numbers.take(5)}")
    
    # takeOrdered(n) - First n in sorted order
    print(f"5. takeOrdered(5): {numbers.takeOrdered(5)}")
    
    # top(n) - Top n elements (descending)
    print(f"6. top(5): {numbers.top(5)}")
    
    # takeSample() - Random sample
    print(f"7. takeSample(3): {numbers.takeSample(False, 3, seed=42)}")
    
    # reduce() - Aggregate using function
    total = numbers.reduce(lambda a, b: a + b)
    print(f"8. reduce(sum): {total}")
    
    # fold() - Aggregate with initial value
    product = numbers.fold(1, lambda a, b: a * b)
    print(f"9. fold(product): {product}")
    
    # aggregate() - Flexible aggregation
    sum_count = numbers.aggregate(
        (0, 0),
        lambda acc, val: (acc[0] + val, acc[1] + 1),
        lambda acc1, acc2: (acc1[0] + acc2[0], acc1[1] + acc2[1])
    )
    print(f"10. aggregate(sum, count): {sum_count}")
    
    # max() and min()
    print(f"11. max(): {numbers.max()}, min(): {numbers.min()}")
    
    # mean() and stdev()
    print(f"12. mean(): {numbers.mean()}, stdev(): {numbers.stdev():.2f}")
    
    # sum()
    print(f"13. sum(): {numbers.sum()}")
    
    # countByValue()
    data = sc.parallelize([1, 2, 2, 3, 3, 3])
    print(f"14. countByValue(): {dict(data.countByValue())}")


def demonstrate_pair_rdd_operations(sc: SparkContext) -> None:
    """Demonstrate operations on Pair RDDs (key-value pairs)."""
    print("\n" + "=" * 60)
    print("PAIR RDD OPERATIONS")
    print("=" * 60)
    
    pairs = sc.parallelize([
        ("apple", 3), ("banana", 2), ("apple", 5),
        ("orange", 1), ("banana", 4), ("apple", 2)
    ])
    
    # keys() and values()
    print(f"\n1. keys(): {pairs.keys().collect()}")
    print(f"2. values(): {pairs.values().collect()}")
    
    # reduceByKey() - Aggregate values by key
    sum_by_fruit = pairs.reduceByKey(lambda a, b: a + b)
    print(f"3. reduceByKey(sum): {sum_by_fruit.collect()}")
    
    # groupByKey() - Group values by key
    grouped = pairs.groupByKey()
    result = [(k, list(v)) for k, v in grouped.collect()]
    print(f"4. groupByKey(): {result}")
    
    # mapValues() - Apply function to values only
    doubled = pairs.mapValues(lambda v: v * 2)
    print(f"5. mapValues(x2): {doubled.collect()}")
    
    # flatMapValues() - FlatMap on values
    expanded = pairs.flatMapValues(lambda v: range(v))
    print(f"6. flatMapValues(range): {expanded.take(10)}")
    
    # sortByKey() - Sort by key
    sorted_pairs = pairs.sortByKey()
    print(f"7. sortByKey(): {sorted_pairs.collect()}")
    
    # countByKey() - Count by key
    print(f"8. countByKey(): {dict(pairs.countByKey())}")
    
    # Join operations
    rdd1 = sc.parallelize([("a", 1), ("b", 2), ("c", 3)])
    rdd2 = sc.parallelize([("a", 10), ("b", 20), ("d", 40)])
    
    # join() - Inner join
    inner = rdd1.join(rdd2)
    print(f"9. join(): {inner.collect()}")
    
    # leftOuterJoin()
    left = rdd1.leftOuterJoin(rdd2)
    print(f"10. leftOuterJoin(): {left.collect()}")
    
    # rightOuterJoin()
    right = rdd1.rightOuterJoin(rdd2)
    print(f"11. rightOuterJoin(): {right.collect()}")
    
    # fullOuterJoin()
    full = rdd1.fullOuterJoin(rdd2)
    print(f"12. fullOuterJoin(): {full.collect()}")
    
    # cogroup() - Group by key across RDDs
    cogrouped = rdd1.cogroup(rdd2)
    result = [(k, (list(v1), list(v2))) for k, (v1, v2) in cogrouped.collect()]
    print(f"13. cogroup(): {result}")


def demonstrate_persistence(sc: SparkContext) -> None:
    """Demonstrate RDD caching and persistence."""
    print("\n" + "=" * 60)
    print("RDD PERSISTENCE AND CACHING")
    print("=" * 60)
    
    # Create an RDD with some computation
    data = sc.parallelize(range(100000))
    computed = data.map(lambda x: x ** 2).filter(lambda x: x % 2 == 0)
    
    # cache() - Store in memory
    computed.cache()
    print(f"\n1. Cached RDD - is_cached: {computed.is_cached}")
    
    # First action (computes and caches)
    import time
    start = time.time()
    count1 = computed.count()
    time1 = time.time() - start
    print(f"2. First count (computed): {count1} in {time1:.3f}s")
    
    # Second action (uses cache)
    start = time.time()
    count2 = computed.count()
    time2 = time.time() - start
    print(f"3. Second count (cached): {count2} in {time2:.3f}s")
    
    # unpersist()
    computed.unpersist()
    print(f"4. After unpersist - is_cached: {computed.is_cached}")
    
    # Storage levels
    print("\n5. Available Storage Levels:")
    print(f"   - MEMORY_ONLY: {StorageLevel.MEMORY_ONLY}")
    print(f"   - MEMORY_AND_DISK: {StorageLevel.MEMORY_AND_DISK}")
    print(f"   - DISK_ONLY: {StorageLevel.DISK_ONLY}")
    print(f"   - MEMORY_ONLY_SER: {StorageLevel.MEMORY_ONLY_SER}")
    print(f"   - MEMORY_AND_DISK_SER: {StorageLevel.MEMORY_AND_DISK_SER}")


def demonstrate_partitioning(sc: SparkContext) -> None:
    """Demonstrate RDD partitioning operations."""
    print("\n" + "=" * 60)
    print("RDD PARTITIONING")
    print("=" * 60)
    
    data = sc.parallelize(range(100), numSlices=4)
    
    # getNumPartitions()
    print(f"\n1. Initial partitions: {data.getNumPartitions()}")
    
    # glom() - View elements per partition
    partitions = data.glom().collect()
    print(f"2. Elements per partition: {[len(p) for p in partitions]}")
    
    # repartition() - Change number of partitions (shuffle)
    repartitioned = data.repartition(8)
    print(f"3. After repartition(8): {repartitioned.getNumPartitions()}")
    
    # coalesce() - Reduce partitions (no shuffle)
    coalesced = repartitioned.coalesce(2)
    print(f"4. After coalesce(2): {coalesced.getNumPartitions()}")
    
    # partitionBy() - Partition by key (for pair RDDs)
    pairs = sc.parallelize([(i % 3, i) for i in range(12)])
    partitioned = pairs.partitionBy(3)
    print(f"5. partitionBy(3) partitions: {partitioned.getNumPartitions()}")
    
    # View partitioned data
    partitioned_data = partitioned.glom().collect()
    for i, partition in enumerate(partitioned_data):
        print(f"   Partition {i}: {partition}")


def demonstrate_lineage(sc: SparkContext) -> None:
    """Demonstrate RDD lineage and DAG."""
    print("\n" + "=" * 60)
    print("RDD LINEAGE (DAG)")
    print("=" * 60)
    
    # Create a chain of transformations
    rdd1 = sc.parallelize([1, 2, 3, 4, 5])
    rdd2 = rdd1.map(lambda x: x * 2)
    rdd3 = rdd2.filter(lambda x: x > 4)
    rdd4 = rdd3.map(lambda x: (x, x ** 2))
    
    # View lineage
    print("\nRDD Lineage (toDebugString):")
    print(rdd4.toDebugString().decode('utf-8'))
    
    # Execute and show result
    print(f"\nFinal result: {rdd4.collect()}")


def word_count_example(sc: SparkContext) -> None:
    """Classic word count example using RDDs."""
    print("\n" + "=" * 60)
    print("WORD COUNT EXAMPLE")
    print("=" * 60)
    
    # Sample text
    text = [
        "Apache Spark is a unified analytics engine",
        "Spark provides high-level APIs in Java Scala Python and R",
        "Spark also supports SQL and streaming data",
        "Spark runs on Hadoop YARN Kubernetes and standalone"
    ]
    
    lines = sc.parallelize(text)
    
    # Word count pipeline
    word_counts = (
        lines
        .flatMap(lambda line: line.lower().split())  # Split into words
        .map(lambda word: (word, 1))                  # Create pairs
        .reduceByKey(lambda a, b: a + b)              # Sum counts
        .sortBy(lambda x: x[1], ascending=False)      # Sort by count
    )
    
    print("\nWord counts (top 10):")
    for word, count in word_counts.take(10):
        print(f"  {word}: {count}")


def main():
    """Main function to run all demonstrations."""
    print("\n" + "=" * 60)
    print("PYSPARK RDD BASICS - COMPREHENSIVE GUIDE")
    print("=" * 60)
    
    # Create SparkContext
    sc = create_spark_context()
    
    try:
        # Run all demonstrations
        demonstrate_rdd_creation(sc)
        demonstrate_transformations(sc)
        demonstrate_actions(sc)
        demonstrate_pair_rdd_operations(sc)
        demonstrate_persistence(sc)
        demonstrate_partitioning(sc)
        demonstrate_lineage(sc)
        word_count_example(sc)
        
        print("\n" + "=" * 60)
        print("ALL DEMONSTRATIONS COMPLETED SUCCESSFULLY!")
        print("=" * 60)
        
    finally:
        # Clean up
        sc.stop()


if __name__ == "__main__":
    main()
