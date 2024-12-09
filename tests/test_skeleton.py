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


def test_concat_dataframe_method(client):
    print('... Testing direct dataframe concatenation')

    # Create initial dataframe
    base_df = pd.DataFrame({
        'transaction_date': pd.date_range(start='2024-01-01', periods=3),
        'user_id': range(1000, 1003),
        'amount': [100, 200, 300],
        'category': ['A', 'B', 'A']
    })

    test_key = 'testZ/concat-direct-test'

    # Upload initial dataframe
    print('... Creating base dataframe')
    start_time = time.time()
    result = client.post_dataframe(
        df=base_df,
        dataframe_name=test_key
    )
    print(f"Base dataframe created in {round(time.time() - start_time, 2)} seconds")
    assert result.get('key')

    # Test small concat
    print('... Testing small concatenation')
    new_small_df = pd.DataFrame({
        'transaction_date': [
            '2024-01-04T00:00:00.000Z',
            '2024-01-05T00:00:00.000Z'
        ],
        'user_id': range(1003, 1005),
        'amount': [400, 500],
        'category': ['C', 'B']
    })

    start_time = time.time()
    client.concat_dataframe(
        new_df=new_small_df,
        dataframe_name=test_key
    )
    print(f"Small concat completed in {round(time.time() - start_time, 2)} seconds")

    # Verify small concat
    result_df = client.get_dataframe(test_key)
    assert len(result_df) == len(base_df) + len(new_small_df)
    print(f"Small concat verification successful: {len(result_df)} total rows")

    # Test larger concat
    print('... Testing larger concatenation')
    larger_dates = pd.date_range(start='2024-01-06', periods=1000)
    iso_dates = [d.strftime('%Y-%m-%dT%H:%M:%S.000Z') for d in larger_dates]

    new_large_df = pd.DataFrame({
        'transaction_date': iso_dates,
        'user_id': range(1005, 2005),
        'amount': [100] * 1000,
        'category': ['A'] * 1000
    })

    start_time = time.time()
    client.concat_dataframe(
        new_df=new_large_df,
        dataframe_name=test_key,
        chunk_size=10 * 1024 * 1024  # 10MB chunks for larger data
    )
    concat_time = time.time() - start_time
    print(f"Large concat completed in {round(concat_time, 2)} seconds")

    # Verify larger concat
    result_df = client.get_dataframe(test_key)
    expected_length = len(base_df) + len(new_small_df) + len(new_large_df)
    assert len(result_df) == expected_length
    print(f"Large concat verification successful: {len(result_df)} total rows")
    print(f"Final performance: {round(len(new_large_df) / concat_time)} rows concatenated per second")

    # Test data integrity
    print('... Verifying data integrity')
    assert set(result_df.columns) == {'transaction_date', 'user_id', 'amount', 'category'}
    assert result_df['user_id'].nunique() == expected_length  # All user_ids should be unique
    assert all(result_df['amount'].notna())  # No NaN values in amount
    assert all(result_df['category'].isin(['A', 'B', 'C']))  # Valid categories only
    print("Data integrity verification successful")


def test_delete_folder(client):
    print('... Testing folder deletion')

    # First create some test data in a folder structure
    test_folder = 'testZ/delete-test-folder'

    # Create a few test files in the folder
    files_df = pd.DataFrame({
        'transaction_date': pd.date_range(start='2024-01-01', periods=3),
        'user_id': range(1000, 1003),
        'amount': [100, 200, 300],
        'category': ['A', 'B', 'A']
    })

    # Upload multiple files to test folder
    for i in range(3):
        start_time = time.time()
        result = client.post_dataframe(
            df=files_df,
            dataframe_name=f'{test_folder}/file{i}',
            chunk_size=5 * 1024 * 1024
        )
        print(f"Created test file {i} in {round(time.time() - start_time, 2)} seconds")
        assert result.get('key')

    # Verify files exist
    try:
        for i in range(3):
            df = client.get_dataframe(f'{test_folder}/file{i}')
            assert not df.empty
        print("Test files created successfully")
    except Exception as e:
        print(f"Failed to verify test files: {str(e)}")
        raise

    # Delete the folder
    print('... Deleting test folder')
    start_time = time.time()
    result = client.delete_folder(test_folder)
    print(f"Folder deletion completed in {round(time.time() - start_time, 2)} seconds")
    print(f"Deleted {result.get('objects_deleted', 0)} objects")

    # Verify deletion
    time.sleep(2)  # Small delay to ensure deletion propagates
    try:
        print('Check if the file does no longer exists')
        df = client.get_dataframe(f'{test_folder}/file0')
        print("Error: Folder still exists")
        assert False
    except Exception as e:
        if '404' in str(e) or 'not found' in str(e).lower():
            print("Folder successfully deleted")
        else:
            raise


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

        print("Testing direct dataframe concatenation...")
        test_concat_dataframe_method(client)

        print("Testing folder deletion...")
        test_delete_folder(client)

    except Exception as e:
        print(f"Test failed: {str(e)}")
        raise


if __name__ == '__main__':
    main()
