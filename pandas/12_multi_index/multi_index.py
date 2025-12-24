"""
Multi-Index - Pandas Learning Hub
=================================
This module demonstrates MultiIndex operations in Pandas.
"""

import pandas as pd


def demonstrate_multi_index():
    """Demonstrate MultiIndex operations."""
    print("\n" + "=" * 60)
    print("MULTI-INDEX OPERATIONS")
    print("=" * 60)
    
    index = pd.MultiIndex.from_product(
        [['A', 'B'], [1, 2]], 
        names=['letter', 'number']
    )
    df = pd.DataFrame({'value': [10, 20, 30, 40]}, index=index)
    
    print("\nMultiIndex DataFrame:")
    print(df)
    
    print("\nSelect 'A':")
    print(df.loc['A'])
    
    print("\nCross-section (number=1):")
    print(df.xs(1, level='number'))


def main():
    demonstrate_multi_index()
    print("\n" + "=" * 60)
    print("ALL DEMONSTRATIONS COMPLETED!")
    print("=" * 60)


if __name__ == "__main__":
    main()
