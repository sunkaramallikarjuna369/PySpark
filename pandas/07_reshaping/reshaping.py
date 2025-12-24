"""
Reshaping Data - Pandas Learning Hub
====================================
This module demonstrates data reshaping operations in Pandas.
"""

import pandas as pd


def demonstrate_melt():
    """Demonstrate melt operation."""
    print("\n" + "=" * 60)
    print("MELT OPERATION")
    print("=" * 60)
    
    df_wide = pd.DataFrame({
        'name': ['Alice', 'Bob'],
        'math': [90, 85],
        'science': [88, 92]
    })
    
    print("\nWide format:")
    print(df_wide)
    
    df_long = pd.melt(df_wide, id_vars=['name'],
                      var_name='subject', value_name='score')
    print("\nLong format (melted):")
    print(df_long)


def demonstrate_stack():
    """Demonstrate stack/unstack operations."""
    print("\n" + "=" * 60)
    print("STACK/UNSTACK OPERATIONS")
    print("=" * 60)
    
    df = pd.DataFrame({
        'A': [1, 2],
        'B': [3, 4]
    }, index=['x', 'y'])
    
    print("\nOriginal:")
    print(df)
    
    stacked = df.stack()
    print("\nStacked:")
    print(stacked)


def main():
    demonstrate_melt()
    demonstrate_stack()
    print("\n" + "=" * 60)
    print("ALL DEMONSTRATIONS COMPLETED!")
    print("=" * 60)


if __name__ == "__main__":
    main()
