"""
Pivot Tables - Pandas Learning Hub
==================================
This module demonstrates pivot table operations in Pandas.
"""

import pandas as pd


def demonstrate_pivot():
    """Demonstrate pivot operations."""
    print("\n" + "=" * 60)
    print("PIVOT OPERATIONS")
    print("=" * 60)
    
    df = pd.DataFrame({
        'date': ['2023-01', '2023-01', '2023-02', '2023-02'],
        'product': ['A', 'B', 'A', 'B'],
        'sales': [100, 150, 120, 180]
    })
    
    print("\nOriginal DataFrame:")
    print(df)
    
    pivoted = df.pivot(index='date', columns='product', values='sales')
    print("\nPivoted:")
    print(pivoted)


def demonstrate_pivot_table():
    """Demonstrate pivot_table with aggregation."""
    print("\n" + "=" * 60)
    print("PIVOT TABLE WITH AGGREGATION")
    print("=" * 60)
    
    df = pd.DataFrame({
        'date': ['2023-01', '2023-01', '2023-02'],
        'product': ['A', 'A', 'B'],
        'sales': [100, 150, 200]
    })
    
    pt = pd.pivot_table(df, values='sales', index='date',
                        columns='product', aggfunc='sum', fill_value=0)
    print("\nPivot table:")
    print(pt)


def main():
    demonstrate_pivot()
    demonstrate_pivot_table()
    print("\n" + "=" * 60)
    print("ALL DEMONSTRATIONS COMPLETED!")
    print("=" * 60)


if __name__ == "__main__":
    main()
