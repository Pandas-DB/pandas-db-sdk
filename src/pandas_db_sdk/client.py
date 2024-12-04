from concurrent.futures import ThreadPoolExecutor
import sys
from math import ceil
from typing import Dict, Optional, Any, Union, List, Tuple
from dataclasses import dataclass
import requests
import boto3
import pandas as pd
import numpy as np
from datetime import datetime
import json
from urllib.parse import quote


# TODO this can go to an aux.py
def convert_numpy_types(value):
    """Convert numpy types to native Python types"""
    import numpy as np
    if isinstance(value, (np.int_, np.intc, np.intp, np.int8,
                          np.int16, np.int32, np.int64, np.uint8, np.uint16,
                          np.uint32, np.uint64)):
        return int(value)
    elif isinstance(value, (np.float_, np.float16, np.float32, np.float64)):
        return float(value)
    elif isinstance(value, (np.bool_)):
        return bool(value)
    elif isinstance(value, (np.ndarray,)):
        return value.tolist()
    return value


class DataFrameClientError(Exception): pass


class AuthenticationError(DataFrameClientError): pass


class APIError(DataFrameClientError): pass


@dataclass
class Filter:
    column: str
    partition_type: str
    value: Optional[str] = None


@dataclass
class DateRangeFilter:
    column: str
    start_date: Optional[str] = None
    end_date: Optional[str] = None
    partition_type: str = 'date'
    value: Optional[str] = None

    def to_filter_params(self) -> Dict[str, str]:
        params = {
            'partition_type': self.partition_type,
            'column': self.column
        }
        # Only include dates in params if they are specified
        if self.start_date is not None:
            params['start_date'] = self.start_date
        if self.end_date is not None:
            params['end_date'] = self.end_date
        return params


@dataclass
class LogRangeFilter:
    column: str
    start_timestamp: str
    end_timestamp: str
    partition_type: str = 'log'
    value: Optional[str] = None

    def to_filter_params(self) -> Dict[str, str]:
        return {
            'partition_type': self.partition_type,
            'column': self.column,  # Changed from external_key to column
            'start_timestamp': self.start_timestamp,
            'end_timestamp': self.end_timestamp
        }


@dataclass
class IdFilter:
    column: str
    values: Union[List[Union[int, str]], Union[int, str]] = None
    partition_type: str = 'id'

    def __post_init__(self):
        # Convert single value to list for consistent handling
        if self.values is not None and not isinstance(self.values, list):
            self.values = [self.values]
        # Convert all values to float for consistent comparison
        if self.values:
            self.values = [float(v) for v in self.values]

    def to_filter_params(self) -> Dict[str, Any]:
        return {
            'partition_type': self.partition_type,
            'column': self.column,
            'values': json.dumps(self.values) if self.values else None
        }


@dataclass
class PaginationInfo:
    page_size: int = 100  # Number of chunks to process per request
    page_token: Optional[str] = None  # Token for next page


class DataFrameClient:
    @staticmethod
    def get_auth_token(api_url: str, user: str, password: str, region: str = 'eu-west-1') -> str:
        api_url = api_url.rstrip('/')
        try:
            response = requests.get(f"{api_url}/auth/config")
            response.raise_for_status()
            client_id = response.json()['userPoolClientId']

            cognito = boto3.client('cognito-idp', region_name=region)
            auth = cognito.initiate_auth(
                ClientId=client_id,
                AuthFlow='USER_PASSWORD_AUTH',
                AuthParameters={'USERNAME': user, 'PASSWORD': password}
            )
            return auth['AuthenticationResult']['IdToken']
        except Exception as e:
            raise AuthenticationError(f"Authentication failed: {str(e)}")

    def __init__(
            self,
            api_url: str,
            user: str = None,
            password: str = None,
            auth_token: str = None,
            region: str = 'eu-west-1'
    ):
        self.api_url = api_url.rstrip('/')
        self.region = region
        self.user = user
        self.password = password

        if not auth_token and user and password:
            self._auth_token = self.get_auth_token(api_url, user, password, region)
        else:
            self._auth_token = auth_token

        if not self._auth_token:
            raise ValueError("Either auth_token or user/password required")

        self.headers = {
            'Authorization': f"Bearer {self._auth_token}",
            'Content-Type': 'application/json'
        }

    def _refresh_token_if_needed(self) -> None:
        try:
            response = requests.get(f"{self.api_url}/auth/verify", headers=self.headers)
            if response.status_code == 401 and self.user and self.password:
                self._auth_token = self.get_auth_token(
                    self.api_url, self.user, self.password, self.region
                )
                self.headers['Authorization'] = f"Bearer {self._auth_token}"
            elif response.status_code == 401:
                raise AuthenticationError("Token expired and no refresh credentials")
        except requests.exceptions.RequestException as e:
            raise APIError(f"Token verification failed: {str(e)}")

    def get_dataframe(
            self,
            dataframe_name: str,
            filter_by: Optional[Union[DateRangeFilter, LogRangeFilter, IdFilter]] = None,
            use_last: bool = False,
            pagination: Optional[PaginationInfo] = None
    ) -> pd.DataFrame:
        """
        Get DataFrame with pagination support
        """
        self._refresh_token_if_needed()
        encoded_name = quote(dataframe_name, safe='')

        params = {'use_last': str(use_last).lower()}

        if filter_by:
            params.update(filter_by.to_filter_params())

        if pagination:
            params.update({
                'page_size': str(pagination.page_size),
                'page_token': pagination.page_token
            })

        try:
            response = requests.get(
                f"{self.api_url}/dataframes/get/{encoded_name}",
                headers=self.headers,
                params=params
            )
            response.raise_for_status()
            data = response.json()

            df = pd.DataFrame(data)

            if len(df) > 0:
                # Handle date columns
                if isinstance(filter_by, DateRangeFilter):
                    df[filter_by.column] = pd.to_datetime(df[filter_by.column])

                # Apply client-side filtering if needed
                if isinstance(filter_by, IdFilter) and filter_by.values:
                    df = df[df[filter_by.column].isin(filter_by.values)]
                elif isinstance(filter_by, DateRangeFilter) and filter_by.start_date and filter_by.end_date:
                    mask = (df[filter_by.column] >= filter_by.start_date) & (df[filter_by.column] <= filter_by.end_date)
                    df = df[mask]

            return df

        except requests.exceptions.RequestException as e:
            error = getattr(e.response, 'json', lambda: {'error': str(e)})().get('error', str(e))
            raise APIError(f"Error retrieving DataFrame: {error}")

    def _get_dataframe_size_mb(self, df: pd.DataFrame) -> float:
        """Calculate DataFrame size in MB"""
        return sys.getsizeof(df.to_json(orient='records')) / (1024 * 1024)

    def _split_dataframe(self, df: pd.DataFrame, chunk_size_mb: float = 5.0) -> List[pd.DataFrame]:
        """Split DataFrame into chunks of approximately chunk_size_mb"""
        total_size_mb = self._get_dataframe_size_mb(df)
        if total_size_mb <= chunk_size_mb:
            return [df]

        # Calculate number of rows per chunk based on average row size
        avg_row_size_mb = total_size_mb / len(df)
        rows_per_chunk = int(chunk_size_mb / avg_row_size_mb)

        return [df[i:i + rows_per_chunk] for i in range(0, len(df), rows_per_chunk)]

    def _upload_chunk(
            self,
            df_chunk: pd.DataFrame,
            dataframe_name: str,
            chunk_index: int,
            total_chunks: int,
            columns_keys: Optional[Dict[str, str]] = None,
            storage_method: str = 'concat'
    ) -> Dict:
        """Upload a single chunk of the DataFrame"""
        # Handle date columns
        date_columns = []
        if columns_keys:
            for col, key_type in columns_keys.items():
                if key_type == 'Date':
                    df_chunk[col] = pd.to_datetime(df_chunk[col])
                    date_columns.append(col)

        df_copy = df_chunk.copy()

        # Format date columns
        for date_col in date_columns:
            df_copy[date_col] = df_copy[date_col].dt.strftime('%Y-%m-%d')

        # Convert to records and handle numpy types
        records = df_copy.to_dict(orient='records')
        for record in records:
            for key, value in record.items():
                record[key] = convert_numpy_types(value)

        json_data = json.dumps(records)

        # Add chunk information to the request
        chunk_info = {
            'dataframe': json_data,
            'dataframe_name': dataframe_name,
            'columns_keys': columns_keys or {},
            'storage_method': storage_method,
            'chunk_index': chunk_index,
            'total_chunks': total_chunks
        }

        try:
            response = requests.post(
                f"{self.api_url}/dataframes/upload",
                headers=self.headers,
                json=chunk_info
            )
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            error = getattr(e.response, 'json', lambda: {'error': str(e)})().get('error', str(e))
            raise APIError(f"Error uploading chunk {chunk_index}: {error}")

    def load_dataframe(
            self,
            df: Union[pd.DataFrame, str, Dict],
            dataframe_name: str,
            columns_keys: Optional[Dict[str, str]] = None,
            storage_method: str = 'concat',
            max_workers: int = 10,
            chunk_size_mb: float = 5.0
    ) -> Dict:
        """
        Upload DataFrame with automatic chunking and parallel processing for 'concat' storage method.
        Args:
            df: DataFrame to upload
            dataframe_name: Name for the DataFrame
            columns_keys: Column keys configuration
            storage_method: Storage method ('concat' or other)
            max_workers: Maximum number of concurrent upload threads
            chunk_size_mb: Maximum size of each chunk in MB
        """
        self._refresh_token_if_needed()

        # Input validation and conversion
        if isinstance(df, str):
            df = pd.read_json(df)
        elif isinstance(df, dict):
            df = pd.DataFrame.from_dict(df)
        elif not isinstance(df, pd.DataFrame):
            raise ValueError("df must be DataFrame, JSON string, or dictionary")

        # Validate columns
        if columns_keys:
            for col, key_type in columns_keys.items():
                if col in {'log', 'default'}:
                    raise ValueError(f"Column name '{col}' is reserved")
                if key_type not in ['Date', 'ID']:
                    raise ValueError(f"Invalid key type: {key_type}")
                if col not in df.columns:
                    raise ValueError(f"Column not found: {col}")

        # If not using concat or DataFrame is small, use regular upload
        if storage_method != 'concat' or self._get_dataframe_size_mb(df) <= chunk_size_mb:
            return self._upload_chunk(df, dataframe_name, 0, 1, columns_keys, storage_method)

        # Split DataFrame into chunks
        chunks = self._split_dataframe(df, chunk_size_mb)
        total_chunks = len(chunks)

        # Upload chunks in parallel
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = []
            for i, chunk in enumerate(chunks):
                futures.append(
                    executor.submit(
                        self._upload_chunk,
                        chunk,
                        dataframe_name,
                        i,
                        total_chunks,
                        columns_keys,
                        storage_method
                    )
                )

            # Wait for all uploads to complete and collect results
            results = []
            for future in futures:
                try:
                    results.append(future.result())
                except Exception as e:
                    # Cancel all pending futures if one fails
                    for f in futures:
                        f.cancel()
                    raise e

        # Return the last result (assuming server handles chunk merging)
        return results[-1]

    def get_available_dates(self, dataframe_name: str, date_column: str) -> List[str]:
        self._refresh_token_if_needed()
        encoded_name = quote(dataframe_name, safe='')

        try:
            response = requests.get(
                f"{self.api_url}/dataframes/{encoded_name}",
                headers=self.headers,
                params={'list': 'dates', 'date_column': date_column}
            )
            response.raise_for_status()
            return response.json()['dates']
        except requests.exceptions.RequestException as e:
            error = getattr(e.response, 'json', lambda: {'error': str(e)})().get('error', str(e))
            raise APIError(f"Error getting available dates: {error}")

    def get_log_timestamps(self, dataframe_name: str) -> List[str]:
        self._refresh_token_if_needed()
        encoded_name = quote(dataframe_name, safe='')

        try:
            response = requests.get(
                f"{self.api_url}/dataframes/{encoded_name}",
                headers=self.headers,
                params={'list': 'timestamps'}
            )
            response.raise_for_status()
            return response.json()['timestamps']
        except requests.exceptions.RequestException as e:
            error = getattr(e.response, 'json', lambda: {'error': str(e)})().get('error', str(e))
            raise APIError(f"Error getting log timestamps: {error}")

    def get_chunk_ranges(
            self,
            dataframe_name: str,
            filter_by: Optional[Union[DateRangeFilter, LogRangeFilter, IdFilter]] = None,
            max_size_mb: int = 5
    ) -> List[Tuple[int, int]]:
        """
        Get chunk ranges for a dataframe that can be used for parallel processing.

        Args:
            dataframe_name: Name of the dataframe to get chunk ranges for
            filter_by: Optional filter to apply (only DateRangeFilter is supported)
            max_size_mb: Maximum size of each chunk in MB

        Returns:
            List of tuples containing (start_index, end_index) for each chunk

        Raises:
            APIError: If the request fails
            ValueError: If an unsupported filter type is provided
        """
        self._refresh_token_if_needed()
        encoded_name = quote(dataframe_name, safe='')

        params = {'max_size_mb': str(max_size_mb)}

        # Only DateRangeFilter is supported based on the lambda implementation
        if filter_by:
            if not isinstance(filter_by, DateRangeFilter):
                raise ValueError("Only DateRangeFilter is supported for chunk ranges")

            params.update(filter_by.to_filter_params())

        try:
            response = requests.get(
                f"{self.api_url}/dataframeChunksSize/{encoded_name}",
                headers=self.headers,
                params=params
            )
            response.raise_for_status()
            return response.json()

        except requests.exceptions.RequestException as e:
            error = getattr(e.response, 'json', lambda: {'error': str(e)})().get('error', str(e))
            raise APIError(f"Error retrieving chunk ranges: {error}")

    def list_dates(self, dataframe_name: str, column: str) -> List[str]:
        """
        List available dates for a specific column in a dataframe.

        Args:
            dataframe_name: Name of the dataframe to get dates for
            column: Name of the column to get dates for

        Returns:
            List of dates available for the specified column

        Raises:
            APIError: If the request fails
            ValueError: If column parameter is missing
        """
        self._refresh_token_if_needed()
        encoded_name = quote(dataframe_name, safe='')

        if not column:
            raise ValueError("Column parameter is required")

        try:
            response = requests.get(
                f"{self.api_url}/dataframesDates/{encoded_name}",
                headers=self.headers,
                params={'list': 'dates', 'column': column}
            )
            response.raise_for_status()
            return response.json()['dates']

        except requests.exceptions.RequestException as e:
            error = getattr(e.response, 'json', lambda: {'error': str(e)})().get('error', str(e))
            raise APIError(f"Error listing dates: {error}")