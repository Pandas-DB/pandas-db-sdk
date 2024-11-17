"""
# Example usage:

# Initialize client
client = DataFrameClient(
    api_url='https://api.example.com',
    auth_token='your-cognito-token'
)

# Upload DataFrame with date-based partitioning
df = pd.DataFrame({
    'date': ['2024-01-01', '2024-01-02'],
    'value': [1, 2]
})

metadata = client.load_dataframe(
    df=df,
    dataframe_name='my-test/test1',
    columns_keys={'date': 'Date'},
    external_key='NOW',
    keep_last=True
)

# Get latest version of DataFrame
df_latest = client.get_dataframe(
    dataframe_name='my-test/test1',
    use_last=True
)

# List available DataFrames
dataframes = client.list_dataframes(prefix='my-test/')
"""

from typing import Dict, Optional, Any
import requests

import boto3
import pandas as pd
from datetime import datetime


class DataFrameClient:
    """Client for interacting with the DataFrame storage service"""

    def __init__(
            self,
            api_url: str,
            auth_token: str,
            region: str = 'us-east-1'
    ):
        """
        Initialize DataFrame client

        Args:
            api_url: Base URL for the API
            auth_token: Authentication token (from Cognito)
            region: AWS region
        """
        self.api_url = api_url.rstrip('/')
        self.headers = {
            'Authorization': f"Bearer {auth_token}",
            'Content-Type': 'application/json'
        }
        self.region = region

    def load_dataframe(
            self,
            df: pd.DataFrame,
            dataframe_name: str,
            columns_keys: Optional[Dict[str, str]] = None,
            external_key: str = 'NOW',
            keep_last: bool = False
    ) -> Dict:
        """
        Load DataFrame to storage system

        Args:
            df: Pandas DataFrame to store
            dataframe_name: Name/path for the DataFrame (e.g., 'my-test/test1')
            columns_keys: Optional dictionary mapping column names to key types ('Date' or 'ID')
            external_key: Key for version organization ('NOW' or custom path)
            keep_last: If True, only keeps the latest version

        Returns:
            Dictionary with storage metadata

        Example:
            >>> client = DataFrameClient('https://api.example.com', 'token')
            >>> df = pd.DataFrame({'date': ['2024-01-01'], 'value': [1]})
            >>> client.load_dataframe(
            ...     df,
            ...     'my-test/test1',
            ...     columns_keys={'date': 'Date'},
            ...     external_key='NOW',
            ...     keep_last=True
            ... )
        """
        # Validate inputs
        if not isinstance(df, pd.DataFrame):
            raise ValueError("df must be a pandas DataFrame")

        if not dataframe_name:
            raise ValueError("dataframe_name is required")

        if columns_keys:
            for col, key_type in columns_keys.items():
                if key_type not in ['Date', 'ID']:
                    raise ValueError(f"Invalid key type for {col}: {key_type}")
                if col not in df.columns:
                    raise ValueError(f"Column not found in DataFrame: {col}")

        # Prepare request
        payload = {
            'dataframe': df.to_json(orient='records'),
            'dataframe_name': dataframe_name,
            'columns_keys': columns_keys,
            'external_key': external_key,
            'keep_last': keep_last
        }

        # Make request
        try:
            response = requests.post(
                f"{self.api_url}/dataframes/upload",
                headers=self.headers,
                json=payload
            )

            response.raise_for_status()
            return response.json()

        except requests.exceptions.RequestException as e:
            if hasattr(e.response, 'json'):
                error = e.response.json().get('error', str(e))
            else:
                error = str(e)
            raise Exception(f"Error uploading DataFrame: {error}")

    def get_dataframe(
            self,
            dataframe_name: str,
            external_key: Optional[str] = None,
            use_last: bool = False
    ) -> pd.DataFrame:
        """
        Retrieve DataFrame from storage

        Args:
            dataframe_name: Name/path of the DataFrame to retrieve
            external_key: Optional external key to filter by
            use_last: If True, returns only the latest version

        Returns:
            Retrieved Pandas DataFrame

        Example:
            >>> client = DataFrameClient('https://api.example.com', 'token')
            >>> df = client.get_dataframe('my-test/test1', use_last=True)
        """
        # Prepare query parameters
        params = {}
        if external_key:
            params['external_key'] = external_key
        if use_last:
            params['use_last'] = 'true'

        # Make request
        try:
            response = requests.get(
                f"{self.api_url}/dataframes/{dataframe_name}",
                headers=self.headers,
                params=params
            )

            response.raise_for_status()
            return pd.DataFrame(response.json())

        except requests.exceptions.RequestException as e:
            if hasattr(e.response, 'json'):
                error = e.response.json().get('error', str(e))
            else:
                error = str(e)
            raise Exception(f"Error retrieving DataFrame: {error}")

    def list_dataframes(
            self,
            prefix: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        List available DataFrames

        Args:
            prefix: Optional prefix to filter DataFrames

        Returns:
            Dictionary with DataFrame listings
        """
        params = {'prefix': prefix} if prefix else {}

        try:
            response = requests.get(
                f"{self.api_url}/dataframes",
                headers=self.headers,
                params=params
            )

            response.raise_for_status()
            return response.json()

        except requests.exceptions.RequestException as e:
            if hasattr(e.response, 'json'):
                error = e.response.json().get('error', str(e))
            else:
                error = str(e)
            raise Exception(f"Error listing DataFrames: {error}")
