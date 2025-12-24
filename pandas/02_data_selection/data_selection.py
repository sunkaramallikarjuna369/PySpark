"""
Data Selection - Pandas Learning Hub
====================================
This module demonstrates data selection methods in Pandas.
"""

import pandas as pd


def demonstrate_loc_selection():
    """Demonstrate loc (label-based) selection."""
    print("\n" + "=" * 60)
    print("LOC SELECTION (Label-based)")
    print("=" * 60)
    
    df = pd.DataFrame({
        'name': ['Alice', 'Bob', 'Charlie'],
        'age': [25, 30, 35],
        'salary': [50000, 60000, 75000]
    }, index=['a', 'b', 'c'])
    
    print("\nDataFrame:")
    print(df)
    
    print("\nloc['a']:")
    print(df.loc['a'])
    
    print("\nloc['a':'b', ['name', 'salary']]:")
    print(df.loc['a':'b', ['name', 'salary']])


def demonstrate_iloc_selection():
    """Demonstrate iloc (integer-based) selection."""
    print("\n" + "=" * 60)
    print("ILOC SELECTION (Integer-based)")
    print("=" * 60)
    
    df = pd.DataFrame({
        'name': ['Alice', 'Bob', 'Charlie'],
        'age': [25, 30, 35],
        'salary': [50000, 60000, 75000]
    })
    
    print("\nDataFrame:")
    print(df)
    
    print("\niloc[0]:")
    print(df.iloc[0])
    
    print("\niloc[:2, :2]:")
    print(df.iloc[:2, :2])


def main():
    demonstrate_loc_selection()
    demonstrate_iloc_selection()
    print("\n" + "=" * 60)
    print("ALL DEMONSTRATIONS COMPLETED!")
    print("=" * 60)


if __name__ == "__main__":
    main()
