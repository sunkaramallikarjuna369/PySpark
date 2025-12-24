"""
Apply & Lambda - Pandas Learning Hub
====================================
This module demonstrates apply and lambda operations in Pandas.
"""

import pandas as pd
import numpy as np


def demonstrate_apply():
    """Demonstrate apply operations."""
    print("\n" + "=" * 60)
    print("APPLY OPERATIONS")
    print("=" * 60)
    
    df = pd.DataFrame({
        'name': ['Alice', 'Bob', 'Charlie'],
        'salary': [50000, 60000, 75000]
    })
    
    print("\nOriginal:")
    print(df)
    
    df['salary_category'] = df['salary'].apply(
        lambda x: 'High' if x > 60000 else 'Low'
    )
    print("\nWith category:")
    print(df)


def demonstrate_vectorized():
    """Demonstrate vectorized operations."""
    print("\n" + "=" * 60)
    print("VECTORIZED OPERATIONS")
    print("=" * 60)
    
    df = pd.DataFrame({'A': [1, 2, 3, 4, 5]})
    
    df['B'] = df['A'] * 2
    df['C'] = np.where(df['A'] > 2, 'High', 'Low')
    
    print("\nVectorized operations:")
    print(df)


def main():
    demonstrate_apply()
    demonstrate_vectorized()
    print("\n" + "=" * 60)
    print("ALL DEMONSTRATIONS COMPLETED!")
    print("=" * 60)


if __name__ == "__main__":
    main()
