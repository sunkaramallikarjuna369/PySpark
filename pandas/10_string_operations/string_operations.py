"""
String Operations - Pandas Learning Hub
=======================================
This module demonstrates string operations in Pandas.
"""

import pandas as pd


def demonstrate_string_operations():
    """Demonstrate string operations."""
    print("\n" + "=" * 60)
    print("STRING OPERATIONS")
    print("=" * 60)
    
    df = pd.DataFrame({
        'name': ['Alice Smith', 'Bob Johnson', 'Charlie Brown'],
        'email': ['alice@gmail.com', 'BOB@YAHOO.COM', 'charlie@outlook.com']
    })
    
    print("\nOriginal:")
    print(df)
    
    print("\nLowercase emails:")
    print(df['email'].str.lower())
    
    print("\nFirst names:")
    print(df['name'].str.split().str[0])


def main():
    demonstrate_string_operations()
    print("\n" + "=" * 60)
    print("ALL DEMONSTRATIONS COMPLETED!")
    print("=" * 60)


if __name__ == "__main__":
    main()
