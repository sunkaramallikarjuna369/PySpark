"""
Merge, Join & Concat - Pandas Learning Hub
==========================================
This module demonstrates merge, join, and concat operations in Pandas.
"""

import pandas as pd


def demonstrate_merge():
    """Demonstrate merge operations."""
    print("\n" + "=" * 60)
    print("MERGE OPERATIONS")
    print("=" * 60)
    
    employees = pd.DataFrame({
        'emp_id': [1, 2, 3],
        'name': ['Alice', 'Bob', 'Charlie'],
        'dept_id': [101, 102, 101]
    })
    
    departments = pd.DataFrame({
        'dept_id': [101, 102],
        'dept_name': ['IT', 'HR']
    })
    
    print("\nInner merge:")
    print(pd.merge(employees, departments, on='dept_id'))
    
    print("\nLeft merge:")
    print(pd.merge(employees, departments, on='dept_id', how='left'))


def demonstrate_concat():
    """Demonstrate concat operations."""
    print("\n" + "=" * 60)
    print("CONCAT OPERATIONS")
    print("=" * 60)
    
    df1 = pd.DataFrame({'A': [1, 2], 'B': [3, 4]})
    df2 = pd.DataFrame({'A': [5, 6], 'B': [7, 8]})
    
    print("\nVertical concat:")
    print(pd.concat([df1, df2], ignore_index=True))


def main():
    demonstrate_merge()
    demonstrate_concat()
    print("\n" + "=" * 60)
    print("ALL DEMONSTRATIONS COMPLETED!")
    print("=" * 60)


if __name__ == "__main__":
    main()
