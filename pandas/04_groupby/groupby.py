"""
GroupBy Operations - Pandas Learning Hub
========================================
This module demonstrates GroupBy operations in Pandas.
"""

import pandas as pd


def demonstrate_groupby():
    """Demonstrate GroupBy operations."""
    print("\n" + "=" * 60)
    print("GROUPBY OPERATIONS")
    print("=" * 60)
    
    df = pd.DataFrame({
        'dept': ['IT', 'HR', 'IT', 'HR', 'IT'],
        'name': ['Alice', 'Bob', 'Charlie', 'Diana', 'Eve'],
        'salary': [50000, 60000, 75000, 55000, 45000]
    })
    
    print("\nDataFrame:")
    print(df)
    
    print("\nMean salary by dept:")
    print(df.groupby('dept')['salary'].mean())
    
    print("\nMultiple aggregations:")
    print(df.groupby('dept')['salary'].agg(['mean', 'min', 'max']))


def main():
    demonstrate_groupby()
    print("\n" + "=" * 60)
    print("ALL DEMONSTRATIONS COMPLETED!")
    print("=" * 60)


if __name__ == "__main__":
    main()
