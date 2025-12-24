"""
Missing Data - Pandas Learning Hub
==================================
This module demonstrates handling missing data in Pandas.
"""

import pandas as pd
import numpy as np


def demonstrate_detect_missing():
    """Demonstrate detecting missing values."""
    print("\n" + "=" * 60)
    print("DETECTING MISSING VALUES")
    print("=" * 60)
    
    df = pd.DataFrame({
        'A': [1, 2, np.nan, 4],
        'B': [np.nan, 2, 3, 4]
    })
    
    print("\nDataFrame:")
    print(df)
    
    print("\nMissing count per column:")
    print(df.isnull().sum())


def demonstrate_fill_missing():
    """Demonstrate filling missing values."""
    print("\n" + "=" * 60)
    print("FILLING MISSING VALUES")
    print("=" * 60)
    
    df = pd.DataFrame({'A': [1, np.nan, 3, np.nan, 5]})
    
    print("\nOriginal:")
    print(df)
    
    print("\nFilled with mean:")
    print(df.fillna(df['A'].mean()))


def main():
    demonstrate_detect_missing()
    demonstrate_fill_missing()
    print("\n" + "=" * 60)
    print("ALL DEMONSTRATIONS COMPLETED!")
    print("=" * 60)


if __name__ == "__main__":
    main()
