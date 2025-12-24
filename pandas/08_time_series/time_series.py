"""
Time Series - Pandas Learning Hub
=================================
This module demonstrates time series operations in Pandas.
"""

import pandas as pd
import numpy as np


def demonstrate_datetime():
    """Demonstrate datetime operations."""
    print("\n" + "=" * 60)
    print("DATETIME OPERATIONS")
    print("=" * 60)
    
    dates = pd.date_range('2023-01-01', periods=5, freq='D')
    df = pd.DataFrame({'value': range(5)}, index=dates)
    
    print("\nDataFrame with DatetimeIndex:")
    print(df)
    
    print("\nDate range selection:")
    print(df.loc['2023-01-02':'2023-01-04'])


def demonstrate_resampling():
    """Demonstrate resampling operations."""
    print("\n" + "=" * 60)
    print("RESAMPLING")
    print("=" * 60)
    
    dates = pd.date_range('2023-01-01', periods=24, freq='H')
    df = pd.DataFrame({'value': np.random.randint(1, 100, 24)}, index=dates)
    
    print("\nHourly data (first 5):")
    print(df.head())
    
    print("\nDaily sum:")
    print(df.resample('D').sum())


def demonstrate_rolling():
    """Demonstrate rolling window operations."""
    print("\n" + "=" * 60)
    print("ROLLING WINDOW")
    print("=" * 60)
    
    df = pd.DataFrame({'value': [10, 20, 15, 30, 25]})
    df['rolling_mean'] = df['value'].rolling(window=3).mean()
    
    print("\nWith rolling mean:")
    print(df)


def main():
    demonstrate_datetime()
    demonstrate_resampling()
    demonstrate_rolling()
    print("\n" + "=" * 60)
    print("ALL DEMONSTRATIONS COMPLETED!")
    print("=" * 60)


if __name__ == "__main__":
    main()
