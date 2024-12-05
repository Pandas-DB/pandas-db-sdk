from concurrent.futures import ThreadPoolExecutor
from urllib.parse import quote
import pandas as pd
import boto3
import requests
from typing import Dict, Optional, Any, Union, List, Tuple
import json

from .models import (
    DataFrameClientError,
    AuthenticationError,
    APIError,
    DateRangeFilter,
    LogRangeFilter,
    IdFilter
)
from .auth import AuthManager
from .utils import convert_numpy_types


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
            max_workers: int = 10
    ) -> pd.DataFrame:
        """
        Get DataFrame using chunk ranges for efficient parallel retrieval
        """
        self._refresh_token_if_needed()
        encoded_name = quote(dataframe_name, safe='')

        # 1. Get chunk ranges first
        try:
            ranges = self.get_chunk_ranges(dataframe_name, filter_by)
            if not ranges:
                return pd.DataFrame()
        except Exception as e:
            raise APIError(f"Error getting chunk ranges: {str(e)}")

        # 2. Retrieve chunks in parallel
        dfs = []
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Prepare parameters for each range
            futures = []
            for start_idx, end_idx in ranges:
                params = {'use_last': str(use_last).lower()}
                params.update({'chunk_range': json.dumps([start_idx, end_idx])})
                if filter_by:
                    params.update(filter_by.to_filter_params())

                futures.append(
                    executor.submit(
                        self._get_chunk,
                        encoded_name,
                        params
                    )
                )

            # Collect results maintaining order
            for future in futures:
                try:
                    chunk_df = future.result()
                    if len(chunk_df) > 0:
                        dfs.append(chunk_df)
                except Exception as e:
                    # Cancel remaining futures if one fails
                    for f in futures:
                        f.cancel()
                    raise APIError(f"Error retrieving DataFrame chunk: {str(e)}")

        # Combine all chunks
        if not dfs:
            return pd.DataFrame()

        result_df = pd.concat(dfs, ignore_index=True)

        # Apply any client-side filtering if needed
        if len(result_df) > 0:
            if isinstance(filter_by, DateRangeFilter):
                result_df[filter_by.column] = pd.to_datetime(result_df[filter_by.column])
                if filter_by.start_date and filter_by.end_date:
                    mask = (result_df[filter_by.column] >= filter_by.start_date) & \
                           (result_df[filter_by.column] <= filter_by.end_date)
                    result_df = result_df[mask]
            elif isinstance(filter_by, IdFilter) and filter_by.values:
                result_df = result_df[result_df[filter_by.column].isin(filter_by.values)]

        return result_df

    def _get_chunk(self, encoded_name: str, params: Dict[str, Any]) -> pd.DataFrame:
        """Helper method to retrieve a single chunk"""
        try:
            response = requests.get(
                f"{self.api_url}/dataframes/get/{encoded_name}",
                headers=self.headers,
                params=params
            )
            response.raise_for_status()
            data = response.json()
            return pd.DataFrame(data)
        except requests.exceptions.RequestException as e:
            error = getattr(e.response, 'json', lambda: {'error': str(e)})().get('error', str(e))
            raise APIError(f"Error retrieving DataFrame chunk: {error}")

    def _get_dataframe_size_mb(self, df: pd.DataFrame) -> float:
        """Calculate DataFrame size in MB more accurately"""
        # Convert to JSON the same way we'll send it
        records = df.to_dict(orient='records')
        for record in records:
            for key, value in record.items():
                record[key] = convert_numpy_types(value)

        json_data = json.dumps(records)
        return len(json_data.encode('utf-8')) / (1024 * 1024)

    def _split_dataframe(self, df: pd.DataFrame, chunk_size_mb: float = 5.0) -> List[pd.DataFrame]:
        """Split DataFrame into chunks of approximately chunk_size_mb"""
        total_size_mb = self._get_dataframe_size_mb(df)
        if total_size_mb <= chunk_size_mb:
            return [df]

        # Calculate approximate number of rows per chunk
        # Use a safety factor of 0.8 to account for metadata overhead
        safe_chunk_size_mb = chunk_size_mb * 0.8
        rows_per_chunk = max(1, int((safe_chunk_size_mb / total_size_mb) * len(df)))

        chunks = []
        for i in range(0, len(df), rows_per_chunk):
            chunk = df[i:i + rows_per_chunk]
            # Verify chunk size and adjust if needed
            while self._get_dataframe_size_mb(chunk) > safe_chunk_size_mb and len(chunk) > 1:
                # Reduce chunk size by 10% if it's still too large
                rows_per_chunk = max(1, int(rows_per_chunk * 0.9))
                chunk = df[i:i + rows_per_chunk]
            chunks.append(chunk)

        return chunks

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
            chunk_size_mb: float = 4.5
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

        # Handle timestamp columns before any processing
        date_columns = []
        if columns_keys:
            for col, key_type in columns_keys.items():
                if col in {'log', 'default'}:
                    raise ValueError(f"Column name '{col}' is reserved")
                if key_type not in ['Date', 'ID']:
                    raise ValueError(f"Invalid key type: {key_type}")
                if col not in df.columns:
                    raise ValueError(f"Column not found: {col}")
                if key_type == 'Date':
                    df[col] = pd.to_datetime(df[col])
                    date_columns.append(col)

        # Create a copy to avoid modifying the original DataFrame
        df_copy = df.copy()

        # Convert timestamps to string format
        for col in df_copy.select_dtypes(include=['datetime64[ns]']).columns:
            df_copy[col] = df_copy[col].dt.strftime('%Y-%m-%d')

        # Convert numeric columns to Python native types
        for col in df_copy.select_dtypes(include=['integer', 'floating']).columns:
            df_copy[col] = df_copy[col].map(convert_numpy_types)

        # If not using concat or DataFrame is small, use regular upload
        if storage_method != 'concat' or self._get_dataframe_size_mb(df_copy) <= chunk_size_mb:
            return self._upload_chunk(df_copy, dataframe_name, 0, 1, columns_keys, storage_method)

        # Split DataFrame into chunks
        chunks = []
        total_size_mb = self._get_dataframe_size_mb(df_copy)
        rows_per_chunk = max(1, int((chunk_size_mb / total_size_mb) * len(df_copy)))

        for i in range(0, len(df_copy), rows_per_chunk):
            chunk = df_copy[i:i + rows_per_chunk]
            # Verify chunk size and adjust if needed
            while self._get_dataframe_size_mb(chunk) > chunk_size_mb and len(chunk) > 1:
                # Reduce chunk size by 10% if it's still too large
                rows_per_chunk = max(1, int(rows_per_chunk * 0.9))
                chunk = df_copy[i:i + rows_per_chunk]
            chunks.append(chunk)

        total_chunks = len(chunks)

        # Upload chunks in parallel
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = []
            for i, chunk in enumerate(chunks):
                # Convert chunk to records and handle numpy types
                records = chunk.to_dict(orient='records')
                for record in records:
                    for key, value in record.items():
                        record[key] = convert_numpy_types(value)

                # Create chunk info
                chunk_info = {
                    'dataframe': json.dumps(records),
                    'dataframe_name': dataframe_name,
                    'columns_keys': columns_keys or {},
                    'storage_method': storage_method,
                    'chunk_index': i,
                    'total_chunks': total_chunks
                }

                # Submit chunk upload task
                futures.append(
                    executor.submit(
                        self._make_upload_request,
                        chunk_info
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

        if filter_by:
            if not isinstance(filter_by, (DateRangeFilter, IdFilter)):
                raise ValueError("Only DateRangeFilter and IdFilter are supported for chunk ranges")

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

    def _make_upload_request(self, chunk_info: Dict[str, Any]) -> Dict[str, Any]:
        """Helper method to make the actual upload request"""
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
            raise APIError(f"Error uploading chunk {chunk_info['chunk_index']}: {error}")
