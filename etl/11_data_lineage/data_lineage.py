"""
Data Lineage - ETL Learning Hub
===============================
This module demonstrates data lineage concepts using PySpark.
"""

from pyspark.sql import SparkSession
from datetime import datetime
from typing import List, Dict
import json


class LineageTracker:
    """Simple lineage tracker for demonstration."""
    
    def __init__(self):
        self.events = []
    
    def track_read(self, source: str):
        self.events.append({
            "operation": "READ",
            "source": source,
            "timestamp": datetime.now().isoformat()
        })
    
    def track_transform(self, inputs: List[str], output: str, expression: str):
        self.events.append({
            "operation": "TRANSFORM",
            "inputs": inputs,
            "output": output,
            "expression": expression,
            "timestamp": datetime.now().isoformat()
        })
    
    def track_write(self, target: str, sources: List[str]):
        self.events.append({
            "operation": "WRITE",
            "target": target,
            "sources": sources,
            "timestamp": datetime.now().isoformat()
        })
    
    def get_lineage(self) -> List[Dict]:
        return self.events


def demonstrate_lineage(spark):
    """Demonstrate data lineage tracking."""
    print("\n" + "=" * 60)
    print("DATA LINEAGE")
    print("=" * 60)
    
    tracker = LineageTracker()
    
    # Simulate ETL with lineage tracking
    tracker.track_read("customers.csv")
    tracker.track_read("orders.csv")
    
    tracker.track_transform(
        inputs=["first_name", "last_name"],
        output="full_name",
        expression="CONCAT(first_name, ' ', last_name)"
    )
    
    tracker.track_write(
        target="customer_orders",
        sources=["customers", "orders"]
    )
    
    print("\nLineage Events:")
    for event in tracker.get_lineage():
        print(f"  {event['operation']}: {json.dumps(event, indent=4)}")
    
    print("\nLineage Benefits:")
    print("1. Impact Analysis - What breaks if source changes?")
    print("2. Root Cause Analysis - Where did bad data come from?")
    print("3. Compliance - GDPR, CCPA data tracking")
    print("4. Documentation - Auto-generated data flow docs")


def main():
    spark = SparkSession.builder.appName("DataLineage").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    
    demonstrate_lineage(spark)
    
    spark.stop()
    print("\n" + "=" * 60)
    print("ALL DEMONSTRATIONS COMPLETED!")
    print("=" * 60)


if __name__ == "__main__":
    main()
