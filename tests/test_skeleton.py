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

# TODO me falta implementar el get version control (last) y todo lo que haya
    client.get_dataframe('test/transactions/', )

    # Test 5: Get Available Dates
    print("\nTest 5: List Available Dates")
    dates = client.get_available_dates('test/transactions', 'transaction_date')
    print("Available dates:", dates)

    # Test 6: Get Log Timestamps
    print("\nTest 6: List Log Timestamps")
    timestamps = client.get_log_timestamps('test/transactions')
    print("Log timestamps:", timestamps)

    # Test 7: Retrieve by Log Range
    print("\nTest 7: Retrieve by Log Range")
    now = datetime.utcnow()
    log_filter = LogRangeFilter(
        column='log',
        start_timestamp=(now - timedelta(minutes=5)).strftime('%Y-%m-%d_%H:%M:%S'),
        end_timestamp=now.strftime('%Y-%m-%d_%H:%M:%S')
    )
    df_log = client.get_dataframe('test/transactions', filter_by=log_filter)
    print("Retrieved by log range:")
    print(df_log)

if __name__ == '__main__':
    test_dataframe_client()