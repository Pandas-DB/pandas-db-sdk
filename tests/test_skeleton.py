from os import getenv
import pandas as pd
from datetime import datetime, timedelta
from pandas_db_sdk.client import DataFrameClient, DateRangeFilter, LogRangeFilter, IdFilter

def test_dataframe_client():
    # Initialize client
    client = DataFrameClient(
        api_url=getenv('API_URL'),
        user=getenv('USER'),
        password=getenv('PASS')
    )

    # Create sample DataFrame with multiple partition types
    df = pd.DataFrame({
        'transaction_date': pd.date_range(start='2024-01-01', periods=5),
        'user_id': range(1000, 1005),
        'amount': [100, 200, 300, 400, 500],
        'category': ['A', 'B', 'A', 'C', 'B']
    })

    print("Original DataFrame:")
    print(df)

    # Test 1: Basic Upload with Date Partitioning
    print("\nTest 1: Upload with Date Partitioning")
    metadata = client.load_dataframe(
        df=df,
        dataframe_name='test/transactions',
        columns_keys={'transaction_date': 'Date'}
    )
    print("Upload metadata:", metadata)

    # Test 2: Upload with Multiple Partition Types
    print("\nTest 2: Upload with Multiple Partitions")
    metadata = client.load_dataframe(
        df=df,
        dataframe_name='test/transactions-multi',
        columns_keys={
            'transaction_date': 'Date',
            'user_id': 'ID'
        }
    )
    print("Upload metadata:", metadata)

    # Test 3: Retrieve by Date Range
    print("\nTest 3: Retrieve by Date Range")
    date_filter = DateRangeFilter(
        column='transaction_date',
        start_date='2024-01-04',
        end_date='2024-01-08'
    )
    df_date = client.get_dataframe('test/transactions-multi', filter_by=date_filter)
    print("Retrieved by date range:")
    print(df_date)
    assert df_date['transaction_date'].nunique() == 2  # 2024-01-04 and 2024-01-05

    date_filter = DateRangeFilter(column='transaction_date')
    df_date = client.get_dataframe('test/transactions-multi', filter_by=date_filter)
    print("Retrieved by date range:")
    print(df_date)
    assert df_date['transaction_date'].nunique() == df['transaction_date'].nunique()

    # Test 4: Retrieve by ID
    print("\nTest 4: Retrieve by ID")
    # Get multiple specific IDs
    id_filter = IdFilter(
        column='user_id',
        values=[1001, 1002, 1004]
    )
    df_id = client.get_dataframe('test/transactions-multi', filter_by=id_filter)
    print("Retrieved by IDs:")
    print(df_id)
    assert df_id['user_id'].nunique() == 3

    # Or single ID (still works)
    id_filter = IdFilter(
        column='user_id',
        values=1002
    )
    df_id = client.get_dataframe('test/transactions-multi', filter_by=id_filter)
    print("Retrieved by ID:")
    print(df_id)
    assert df_id['user_id'].nunique() == 1

    # or all
    id_filter = IdFilter(column='user_id')
    df_id = client.get_dataframe('test/transactions-multi', filter_by=id_filter)
    print("Retrieved by ID:")
    print(df_id)
    assert  df_id['user_id'].nunique() == df['user_id'].nunique()

    # Test 10 Get chunks ranges
    chunks = client.get_chunk_ranges("test/transactions-multi")
    assert not chunks

    # Get chunks with date filter
    date_filter = DateRangeFilter(
        column="transaction_date",
        start_date="2024-01-01",
        end_date="2024-12-31"
    )
    chunks = client.get_chunk_ranges("test/transactions-multi", filter_by=date_filter)
    assert chunks

    # Test 11: Get Available Dates
    dates = client.list_dates("test/transactions-multi", "transaction_date")
    print("Available dates:", dates)
    assert dates

# TODO me falta implementar el get version control (last) y todo lo que haya
    client.get_dataframe('test/transactions/', )

    # Test 6: Large dataframe. This one is:
    #  Almost 16MB size
    #  Also a large number of files: if stored by Date it will create more than 1000 files
    size = 2 * 10 ** 5
    dates = int(size / 1000) * pd.date_range(start='2024-01-01', periods=1000).to_list()
    df = pd.DataFrame({
        'transaction_date': dates,
        'user_id': range(0, size),
        'amount': [100] * size,
        'category': ['A'] * size,
    })
    metadata = client.load_dataframe(
        df=df,
        dataframe_name='test/large',
        columns_keys={
            'transaction_date': 'Date',
            'user_id': 'ID'
        }
    )
    print("Upload metadata:", metadata)

    date_filter = DateRangeFilter(column='transaction_date')
    df_large = client.get_dataframe('test/large', filter_by=date_filter)
    print("Retrieved by date range:")
    print(df_date)
    assert len(df_large) == size


if __name__ == '__main__':
    test_dataframe_client()