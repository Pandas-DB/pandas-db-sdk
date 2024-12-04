from typing import Dict, Optional, Any, Union, List
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
    start_date: str
    end_date: str
    partition_type: str = 'date'
    value: Optional[str] = None

    def to_filter_params(self) -> Dict[str, str]:
        return {
            'partition_type': self.partition_type,
            'column': self.column,
            'start_date': self.start_date,
            'end_date': self.end_date
        }


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
            use_last: bool = False
    ) -> pd.DataFrame:
        self._refresh_token_if_needed()
        encoded_name = quote(dataframe_name, safe='')

        params = {'use_last': str(use_last).lower()}

        if not filter_by:
            return self._get_single_request(encoded_name, params)

        params.update(filter_by.to_filter_params())

        try:
            if isinstance(filter_by, IdFilter) and filter_by.values:
                # Get all relevant partitions and filter
                all_dfs = []
                response = requests.get(
                    f"{self.api_url}/dataframes/get/{encoded_name}",
                    headers=self.headers,
                    params=params
                )
                response.raise_for_status()
                df = pd.DataFrame(response.json())

                if len(df) > 0:
                    # Filter rows where ID is in the requested values
                    df = df[df[filter_by.column].isin(filter_by.values)]
                    all_dfs.append(df)

                return pd.concat(all_dfs, ignore_index=True) if all_dfs else pd.DataFrame()

            else:
                # Handle date and log filters as before
                response = requests.get(
                    f"{self.api_url}/dataframes/get/{encoded_name}",
                    headers=self.headers,
                    params=params
                )
                response.raise_for_status()
                df = pd.DataFrame(response.json())

                if len(df) > 0:
                    if isinstance(filter_by, DateRangeFilter) and filter_by.start_date and filter_by.end_date:
                        df[filter_by.column] = pd.to_datetime(df[filter_by.column])
                        mask = (df[filter_by.column] >= filter_by.start_date) & (
                                    df[filter_by.column] <= filter_by.end_date)
                        df = df[mask]

                return df

        except requests.exceptions.RequestException as e:
            error = getattr(e.response, 'json', lambda: {'error': str(e)})().get('error', str(e))
            raise APIError(f"Error retrieving DataFrame: {error}")

    def load_dataframe(
            self,
            df: Union[pd.DataFrame, str, Dict],
            dataframe_name: str,
            columns_keys: Optional[Dict[str, str]] = None,
            storage_method: str = 'concat'
    ) -> Dict:
        self._refresh_token_if_needed()

        # Input validation
        if isinstance(df, str):
            try:
                df = pd.read_json(df)
            except Exception as e:
                raise ValueError(f"Invalid JSON string: {str(e)}")
        elif isinstance(df, dict):
            try:
                df = pd.DataFrame.from_dict(df)
            except Exception as e:
                raise ValueError(f"Invalid dictionary format: {str(e)}")
        elif not isinstance(df, pd.DataFrame):
            raise ValueError("df must be DataFrame, JSON string, or dictionary")

        # Column validation
        date_columns = []
        id_columns = []
        if columns_keys:
            for col, key_type in columns_keys.items():
                if col in {'log', 'default'}:
                    raise ValueError(f"Column name '{col}' is reserved")
                if key_type not in ['Date', 'ID']:
                    raise ValueError(f"Invalid key type: {key_type}")
                if col not in df.columns:
                    raise ValueError(f"Column not found: {col}")

                if key_type == 'Date':
                    try:
                        df[col] = pd.to_datetime(df[col])
                        date_columns.append(col)
                    except Exception as e:
                        raise ValueError(f"Invalid date format in {col}: {str(e)}")

                if key_type == 'ID':
                    if not pd.api.types.is_numeric_dtype(df[col]):
                        raise ValueError(f"Column {col} must be numeric")
                    id_columns.append(col)

        # DataFrame to JSON conversion
        try:
            df_copy = df.copy()

            # Handle date columns
            for date_col in date_columns:
                df_copy[date_col] = df_copy[date_col].dt.strftime('%Y-%m-%d')

            # Convert DataFrame to records and handle numpy types
            records = df_copy.to_dict(orient='records')
            for record in records:
                for key, value in record.items():
                    record[key] = convert_numpy_types(value)

            json_data = json.dumps(records)
        except Exception as error:
            raise APIError(f"Error preparing dataframe for upload: {error}")

        # API request
        try:
            response = requests.post(
                f"{self.api_url}/dataframes/upload",
                headers=self.headers,
                json={
                    'dataframe': json_data,
                    'dataframe_name': dataframe_name,
                    'columns_keys': columns_keys or {},
                    'storage_method': storage_method
                }
            )
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            error = getattr(e.response, 'json', lambda: {'error': str(e)})().get('error', str(e))
            raise APIError(f"Error uploading DataFrame: {error}")

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
