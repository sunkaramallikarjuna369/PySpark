"""
Data Types - Pandas Learning Hub
================================
This module demonstrates data type operations in Pandas.
"""

import pandas as pd


def demonstrate_data_types():
    """Demonstrate data type operations."""
    print("\n" + "=" * 60)
    print("DATA TYPES")
    print("=" * 60)
    
    df = pd.DataFrame({
        'int_col': [1, 2, 3],
        'str_col': ['a', 'b', 'c'],
        'float_col': [1.1, 2.2, 3.3]
    })
    
    print("\nData types:")
    print(df.dtypes)
    
    print("\nMemory usage:")
    print(df.memory_usage(deep=True))


def demonstrate_categorical():
    """Demonstrate categorical data type."""
    print("\n" + "=" * 60)
    print("CATEGORICAL DATA TYPE")
    print("=" * 60)
    
    df = pd.DataFrame({'grade': ['A', 'B', 'A', 'C', 'B']})
    df['grade'] = df['grade'].astype('category')
    
    print("\nCategorical dtype:")
    print(df.dtypes)
    print("\nCategories:")
    print(df['grade'].cat.categories)


def main():
    demonstrate_data_types()
    demonstrate_categorical()
    print("\n" + "=" * 60)
    print("ALL DEMONSTRATIONS COMPLETED!")
    print("=" * 60)


if __name__ == "__main__":
    main()
