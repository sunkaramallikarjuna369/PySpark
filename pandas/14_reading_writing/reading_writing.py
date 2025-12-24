"""
Reading & Writing Data - Pandas Learning Hub
============================================
This module demonstrates reading and writing data in Pandas.
"""

import pandas as pd


def demonstrate_csv_operations():
    """Demonstrate CSV read/write operations."""
    print("\n" + "=" * 60)
    print("CSV OPERATIONS")
    print("=" * 60)
    
    # Create sample data
    df = pd.DataFrame({
        'name': ['Alice', 'Bob', 'Charlie'],
        'age': [25, 30, 35],
        'salary': [50000, 60000, 75000]
    })
    
    # Write to CSV
    df.to_csv('/tmp/sample.csv', index=False)
    print("\nWritten to /tmp/sample.csv")
    
    # Read from CSV
    df_read = pd.read_csv('/tmp/sample.csv')
    print("\nRead from CSV:")
    print(df_read)


def demonstrate_json_operations():
    """Demonstrate JSON read/write operations."""
    print("\n" + "=" * 60)
    print("JSON OPERATIONS")
    print("=" * 60)
    
    df = pd.DataFrame({
        'name': ['Alice', 'Bob'],
        'score': [90, 85]
    })
    
    # Write to JSON
    df.to_json('/tmp/sample.json', orient='records', indent=2)
    print("\nWritten to /tmp/sample.json")
    
    # Read from JSON
    df_read = pd.read_json('/tmp/sample.json')
    print("\nRead from JSON:")
    print(df_read)


def main():
    demonstrate_csv_operations()
    demonstrate_json_operations()
    print("\n" + "=" * 60)
    print("ALL DEMONSTRATIONS COMPLETED!")
    print("=" * 60)


if __name__ == "__main__":
    main()
