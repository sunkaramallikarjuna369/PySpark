"""
Performance Tips - Pandas Learning Hub
=====================================
This module demonstrates performance optimization in Pandas.
"""

import pandas as pd
import numpy as np
import time


def demonstrate_vectorization():
    """Demonstrate vectorization benefits."""
    print("\n" + "=" * 60)
    print("VECTORIZATION")
    print("=" * 60)
    
    df = pd.DataFrame({'A': range(100000)})
    
    # Vectorized operation
    start = time.time()
    df['B'] = df['A'] * 2
    print(f"Vectorized: {time.time() - start:.4f}s")
    
    # Using numpy
    start = time.time()
    df['C'] = np.sqrt(df['A'])
    print(f"NumPy sqrt: {time.time() - start:.4f}s")


def demonstrate_memory_optimization():
    """Demonstrate memory optimization."""
    print("\n" + "=" * 60)
    print("MEMORY OPTIMIZATION")
    print("=" * 60)
    
    df = pd.DataFrame({
        'int_col': range(10000),
        'str_col': ['cat_' + str(i % 5) for i in range(10000)]
    })
    
    print("\nBefore optimization:")
    print(df.memory_usage(deep=True))
    
    df['int_col'] = pd.to_numeric(df['int_col'], downcast='integer')
    df['str_col'] = df['str_col'].astype('category')
    
    print("\nAfter optimization:")
    print(df.memory_usage(deep=True))


def main():
    demonstrate_vectorization()
    demonstrate_memory_optimization()
    print("\n" + "=" * 60)
    print("ALL DEMONSTRATIONS COMPLETED!")
    print("=" * 60)


if __name__ == "__main__":
    main()
