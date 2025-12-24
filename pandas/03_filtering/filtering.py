"""
Filtering & Boolean Indexing - Pandas Learning Hub
=================================================
This module demonstrates filtering operations in Pandas.
"""

import pandas as pd


def demonstrate_basic_filtering():
    """Demonstrate basic filtering operations."""
    print("\n" + "=" * 60)
    print("BASIC FILTERING")
    print("=" * 60)
    
    df = pd.DataFrame({
        'name': ['Alice', 'Bob', 'Charlie', 'Diana'],
        'age': [25, 30, 35, 28],
        'salary': [50000, 60000, 75000, 55000]
    })
    
    print("\nDataFrame:")
    print(df)
    
    print("\nAge > 28:")
    print(df[df['age'] > 28])
    
    print("\nSalary between 50000 and 60000:")
    print(df[df['salary'].between(50000, 60000)])


def demonstrate_multiple_conditions():
    """Demonstrate filtering with multiple conditions."""
    print("\n" + "=" * 60)
    print("MULTIPLE CONDITIONS")
    print("=" * 60)
    
    df = pd.DataFrame({
        'name': ['Alice', 'Bob', 'Charlie'],
        'age': [25, 30, 35],
        'dept': ['IT', 'HR', 'IT']
    })
    
    print("\nAge > 25 AND Dept == 'IT':")
    print(df[(df['age'] > 25) & (df['dept'] == 'IT')])


def main():
    demonstrate_basic_filtering()
    demonstrate_multiple_conditions()
    print("\n" + "=" * 60)
    print("ALL DEMONSTRATIONS COMPLETED!")
    print("=" * 60)


if __name__ == "__main__":
    main()
