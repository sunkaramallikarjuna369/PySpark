"""
Series & DataFrames - Pandas Learning Hub
=========================================
This module demonstrates Series and DataFrame operations in Pandas.
"""

import pandas as pd
import numpy as np


def demonstrate_series():
    """Demonstrate Pandas Series operations."""
    print("\n" + "=" * 60)
    print("PANDAS SERIES")
    print("=" * 60)
    
    # Create Series
    s1 = pd.Series([1, 2, 3, 4, 5])
    print("\nSeries from list:")
    print(s1)
    
    s2 = pd.Series([10, 20, 30], index=['a', 'b', 'c'])
    print("\nSeries with custom index:")
    print(s2)
    
    # Series attributes
    print(f"\nSeries attributes:")
    print(f"  dtype: {s1.dtype}")
    print(f"  size: {s1.size}")
    print(f"  shape: {s1.shape}")


def demonstrate_dataframe():
    """Demonstrate Pandas DataFrame operations."""
    print("\n" + "=" * 60)
    print("PANDAS DATAFRAME")
    print("=" * 60)
    
    data = {
        'name': ['Alice', 'Bob', 'Charlie'],
        'age': [25, 30, 35],
        'salary': [50000, 60000, 75000]
    }
    df = pd.DataFrame(data)
    
    print("\nDataFrame:")
    print(df)
    
    print(f"\nDataFrame attributes:")
    print(f"  shape: {df.shape}")
    print(f"  columns: {df.columns.tolist()}")
    
    print("\nDataFrame describe:")
    print(df.describe())


def main():
    """Main function to run all demonstrations."""
    demonstrate_series()
    demonstrate_dataframe()
    print("\n" + "=" * 60)
    print("ALL DEMONSTRATIONS COMPLETED!")
    print("=" * 60)


if __name__ == "__main__":
    main()
