from os import getenv
import pandas as pd
from datetime import datetime, timedelta
import time

from pandas_db_sdk.client import DataFrameClient


def create_test_dataframe(size=200000):
    """Helper to create test dataframe"""
    dates = int(size / 1000) * pd.date_range(start='2024-01-01', periods=1000).to_list()
    return pd.DataFrame({
        'transaction_date': dates,
        'user_id': range(0, size),
        'amount': [100] * size,
        'category': ['A'] * size,
    })


def get_reference_dataset():
    # Test small dataframe with date partitioning
    return pd.DataFrame({
        'transaction_date': pd.date_range(start='2024-01-01', periods=5),
        'user_id': range(1000, 1005),
        'amount': [100, 200, 300, 400, 500],
        'category': ['A', 'B', 'A', 'C', 'B']
    })


def test_post_dataframe(client):
    print('... Upload small dataframe')
    small_df = get_reference_dataset()
    start_time = time.time()
    result = client.post_dataframe(
        df=small_df,
        dataframe_name='testZ/small-file',
        chunk_size=5 * 1024 * 1024
    )
    print(f"Uploaded {len(small_df)} rows | "
          f"In: {round(time.time() - start_time)} seconds | "
          f"Avg performance: {round(len(small_df) / (time.time() - start_time))} rows uploaded per second")
    assert result.get('key')

    print('... Upload large dataframe')
    large_df = create_test_dataframe()
    start_time = time.time()
    result = client.post_dataframe(
        df=large_df,
        dataframe_name='testZ/large-file',
        chunk_size=10 * 1024 * 1024
    )
    print(f"Uploaded {len(large_df)} rows | "
          f"In: {round(time.time() - start_time)} seconds | "
          f"Avg performance: {round(len(large_df) / (time.time() - start_time))} rows uploaded per second")
    assert result.get('key')

# TODO esto no esta implementado
    print('... Upload multiple dataframes async')
    dfs_to_upload = [
        (small_df, 'testZ/multi-upload-1'),
        (small_df, 'testZ/multi-upload-2'),
        (small_df, 'testZ/multi-upload-3')
    ]
    start_time = time.time()
    results = client.post_dataframe_async(
        dfs=dfs_to_upload,
        chunk_size=5 * 1024 * 1024,
        max_workers=3
    )
    print(f"Uploaded {len(dfs_to_upload)} files | "
          f"In: {time.time() - start_time} seconds | "
          f"Avg performance: {len(dfs_to_upload)} / {time.time() - start_time} files uploaded per second")
    assert len(results) == len(dfs_to_upload)


def test_get_dataframe(client):
    # Take small dataframe
    print('... Retrieve small dataframe')
    start_time = time.time()
    # Single file query
    df = client.get_dataframe('testZ/small-file')
    print(f"Retrieved {len(df)} rows | "
          f"In: {round(time.time() - start_time, 2)} seconds | "
          f"Avg performance: {round(len(df) / (time.time() - start_time))} rows retrieved per second")
    assert not df.empty

    print('... Retrieve large dataframe with query')
    start_time = time.time()
    # Single file query
    df = client.get_dataframe(
        dataframe_name='testZ/large-file',
        query="SELECT * FROM s3object WHERE transaction_date = '2024-01-01'",
    )
    print(f"Retrieved {len(df)} rows | "
          f"In: {round(time.time() - start_time, 2)} seconds | "
          f"Avg performance: {round(len(df) / (time.time() - start_time))} rows retrieved per second")
# TODO no devuelve el df con los nombres de las columnas!!
    assert df['transaction_date'].unique().values == '2024-01-01'

# TODO este falla
    # Take the whole large dataframe
    print('... Retrieve large dataframe')
    start_time = time.time()
    # Single file query
    df = client.get_dataframe('testZ/large-file')
    print(f"Retrieved {len(df)} rows | "
          f"In: {round(time.time() - start_time, 2)} seconds | "
          f"Avg performance: {round(len(df) / (time.time() - start_time))} rows retrieved per second")
    assert df == create_test_dataframe()


def main():
    # Initialize client
    client = DataFrameClient(
        api_url=getenv('API_URL'),
        user=getenv('USER'),
        password=getenv('PASS')
    )

    try:
# TODO descomentar
        # print("Testing posting data...")
        # test_post_dataframe(client)

        print("Testing data retrieval...")
        test_get_dataframe(client)

    except Exception as e:
        print(f"Test failed: {str(e)}")
        raise

if __name__ == '__main__':
    main()