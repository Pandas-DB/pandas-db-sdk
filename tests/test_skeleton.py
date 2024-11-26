from os import getenv

from pandas_db_sdk.client import DataFrameClient
import pandas as pd


user = getenv('USER')
password = getenv('PASS')
api_url = getenv('API_URL')

# Initialize client with username/password
client = DataFrameClient(api_url=api_url, user=user, password=password)

# Create sample DataFrame
df = pd.DataFrame({
    'date': ['2024-01-01', '2024-01-02'],
    'id': [1, 2],
    'value': [100, 200]
})

# Store DataFrame (automatically partitioned by date)
client.load_dataframe(
    df=df,
    dataframe_name='my-project/dataset1',
    columns_keys={'date': 'Date'}
)

# Retrieve DataFrame
df_retrieved = client.get_dataframe('my-project/dataset1')