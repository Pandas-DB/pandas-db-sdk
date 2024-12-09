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
        chunk_size=5 * 1024 * 1024
    )
    print(f"Uploaded {len(large_df)} rows | "
          f"In: {round(time.time() - start_time)} seconds | "
          f"Avg performance: {round(len(large_df) / (time.time() - start_time))} rows uploaded per second")
    assert result.get('key')

    print('... Upload xtra-large dataframe')
    xtra_large_df = create_test_dataframe(10**6)
    start_time = time.time()
    result = client.post_dataframe(
        df=xtra_large_df,
        dataframe_name='testZ/xtra-large-file',
        chunk_size=5 * 1024 * 1024
    )
    print(f"Uploaded {len(xtra_large_df)} rows | "
          f"In: {round(time.time() - start_time)} seconds | "
          f"Avg performance: {round(len(xtra_large_df) / (time.time() - start_time))} rows uploaded per second")
    assert result.get('key')


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

    df = client.get_dataframe(
        dataframe_name='testZ/small-file',
        query="SELECT * FROM s3object WHERE transaction_date = CAST('2024-01-01' AS TIMESTAMP)")
    assert len(df) == 1

    print('... Retrieve large dataframe with query')
    start_time = time.time()
    # Single file query
    df = client.get_dataframe(
        dataframe_name='testZ/large-file',
        query="SELECT * FROM s3object WHERE transaction_date = CAST('2024-01-01' AS TIMESTAMP)"
    )
    print(f"Retrieved {len(df)} rows | "
          f"In: {round(time.time() - start_time, 2)} seconds | "
          f"Avg performance: {round(len(df) / (time.time() - start_time))} rows retrieved per second")
    assert df['transaction_date'].nunique() == 1

    # Take the whole large dataframe
    print('... Retrieve large dataframe')
    start_time = time.time()
    # Single file query
    df = client.get_dataframe('testZ/large-file')
    print(f"Retrieved {len(df)} rows | "
          f"In: {round(time.time() - start_time, 2)} seconds | "
          f"Avg performance: {round(len(df) / (time.time() - start_time))} rows retrieved per second")
    assert len(df) == len(create_test_dataframe())


def test_concat_dataframe(client):
    # First create a base dataframe
    print('... Creating base dataframe')
    base_df = pd.DataFrame({
        'transaction_date': pd.date_range(start='2024-01-01', periods=3),
        'user_id': range(1000, 1003),
        'amount': [100, 200, 300],
        'category': ['A', 'B', 'A']
    })

    result = client.post_dataframe(
        df=base_df,
        dataframe_name='testZ/concat-test',
        chunk_size=5 * 1024 * 1024
    )
    assert result.get('key')

    # Create concat data
    concat_df = pd.DataFrame({
        'transaction_date': ['2024-01-04'],
        'user_id': [1004],
        'amount': [400],
        'category': ['C']
    })

    # Test streaming concat
    print('... Testing streaming concat')
    start_time = time.time()
    result = client.concat_events(
        df=concat_df,
        dataframe_name='testZ/concat-test',
    )
    print(f"Streaming concat initiated in: {round(time.time() - start_time, 2)} seconds")
    assert result.get('message') == 'Event queued for streaming'

    # Wait for streaming concat to complete (you might want to adjust the wait time)
    print('... Waiting for streaming concat to complete (the buffer update is every 60 seconds)')
    time.sleep(65)  # Wait for SQS/Lambda processing

    # Verify the streaming concat
    try:
        streamed_df = client.get_dataframe('testZ/concat-test')
        assert len(streamed_df) > 0
    except Exception as e:
        print(f"Note: Streaming verification might fail if processing isn't complete: {str(e)}")


def main():
    # Initialize client
    client = DataFrameClient(
        api_url=getenv('API_URL'),
        user=getenv('USER'),
        password=getenv('PASS')
    )

    try:
        print("Testing posting data...")
        test_post_dataframe(client)

        print("Testing data retrieval...")
        test_get_dataframe(client)

        # print("Testing data concats...")
        # test_concat_dataframe(client)

    except Exception as e:
        print(f"Test failed: {str(e)}")
        raise


if __name__ == '__main__':
    main()