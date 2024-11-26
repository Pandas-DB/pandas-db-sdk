from typing import Dict, Optional, Any, Union
import requests
import boto3
import pandas as pd
from datetime import datetime
import json
from urllib.parse import quote

class DataFrameClientError(Exception):
    """Base exception for DataFrameClient errors"""
    pass

class AuthenticationError(DataFrameClientError):
    """Raised when authentication fails"""
    pass

class APIError(DataFrameClientError):
    """Raised when API calls fail"""
    pass

class DataFrameClient:
    """Client for interacting with the DataFrame storage service"""

    @staticmethod
    def get_auth_token(api_url: str, user: str, password: str, region: str = 'eu-west-1') -> str:
        """
        Get authentication token using username and password

        Args:
            api_url: Base URL for the API
            user: Cognito username/email
            password: Cognito password
            region: AWS region

        Returns:
            str: Authentication token

        Raises:
            AuthenticationError: If authentication fails
        """
        # Clean API URL
        api_url = api_url.rstrip('/')
        
        # Get User Pool info
        try:
            response = requests.get(f"{api_url}/auth/config")
            response.raise_for_status()
            pool_info = response.json()
            client_id = pool_info['userPoolClientId']
        except requests.exceptions.RequestException as e:
            raise AuthenticationError(f"Error getting authentication configuration: {str(e)}")

        # Authenticate with Cognito
        try:
            cognito = boto3.client('cognito-idp', region_name=region)
            response = cognito.initiate_auth(
                ClientId=client_id,
                AuthFlow='USER_PASSWORD_AUTH',
                AuthParameters={
                    'USERNAME': user,
                    'PASSWORD': password
                }
            )
            return response['AuthenticationResult']['IdToken']
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
        """
        Initialize DataFrame client

        Args:
            api_url: Base URL for the API
            user: Cognito username/email
            password: Cognito password
            auth_token: Optional pre-existing auth token
            region: AWS region

        Raises:
            ValueError: If neither auth_token nor user/password credentials are provided
            AuthenticationError: If authentication fails
        """
        self.api_url = api_url.rstrip('/')
        self.region = region
        self.user = user
        self.password = password
        
        # Get auth token if not provided
        if not auth_token and user and password:
            self._auth_token = self.get_auth_token(api_url, user, password, region)
        else:
            self._auth_token = auth_token
            
        if not self._auth_token:
            raise ValueError("Either auth_token or user/password credentials are required")

        self.headers = {
            'Authorization': f"Bearer {self._auth_token}",
            'Content-Type': 'application/json',
        }

    def _refresh_token_if_needed(self) -> None:
        """
        Check token validity and refresh if needed

        Raises:
            AuthenticationError: If token refresh fails
        """
        try:
            # Try a simple API call to test token
            response = requests.get(
                f"{self.api_url}/auth/verify",
                headers=self.headers
            )
            
            # If unauthorized, try to refresh token
            if response.status_code == 401 and self.user and self.password:
                self._auth_token = self.get_auth_token(
                    self.api_url,
                    self.user,
                    self.password,
                    self.region
                )
                self.headers['Authorization'] = f"Bearer {self._auth_token}"
            elif response.status_code == 401:
                raise AuthenticationError("Token expired and refresh credentials not available")
                
        except requests.exceptions.RequestException as e:
            raise APIError(f"Error verifying token: {str(e)}")

    def load_dataframe(
            self,
            df: Union[pd.DataFrame, str, Dict],
            dataframe_name: str,
            columns_keys: Optional[Dict[str, str]] = None,
            external_key: str = 'NOW',
            keep_last: bool = False
    ) -> Dict:
        """
        Load DataFrame to storage system

        Args:
            df: Pandas DataFrame, JSON string, or dictionary to store
            dataframe_name: Name/path for the DataFrame
            columns_keys: Optional dictionary mapping column names to key types ('Date' or 'ID')
            external_key: Key for version organization ('NOW' or custom path)
            keep_last: If True, only keeps the latest version

        Returns:
            Dict: Storage metadata

        Raises:
            ValueError: If inputs are invalid
            APIError: If API call fails
        """
        self._refresh_token_if_needed()

        # Convert input to DataFrame if needed
        if isinstance(df, str):
            df = pd.read_json(df)
        elif isinstance(df, dict):
            df = pd.DataFrame.from_dict(df)
        elif not isinstance(df, pd.DataFrame):
            raise ValueError("df must be a pandas DataFrame, JSON string, or dictionary")

        # Validate inputs
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
            'columns_keys': columns_keys or {},
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
            raise APIError(f"Error uploading DataFrame: {error}")

    def get_dataframe(
            self,
            dataframe_name: str,
            external_key: Optional[str] = None,
            use_last: bool = False
    ) -> pd.DataFrame:
        """
        Retrieve DataFrame from storage
        """
        self._refresh_token_if_needed()

        # URL encode the dataframe name to handle slashes properly
        encoded_name = quote(dataframe_name, safe='')

        # Prepare query parameters
        params = {}
        if external_key:
            params['external_key'] = external_key
        if use_last:
            params['use_last'] = 'true'

        # Make request with explicit header formatting
        headers = {
            'Authorization': self._auth_token,  # Remove Bearer prefix
            'Content-Type': 'application/json'
        }

        try:
            response = requests.get(
                f"{self.api_url}/dataframes/get/{encoded_name}",  # Updated URL format
                headers=headers,
                params=params
            )

            if not response.ok:
                print(f"Error response: {response.text}")  # Debug logging

            response.raise_for_status()
            return pd.DataFrame(response.json())

        except requests.exceptions.RequestException as e:
            if hasattr(e.response, 'json'):
                try:
                    error = e.response.json().get('error', str(e))
                except:
                    error = e.response.text if e.response else str(e)
            else:
                error = str(e)
            raise APIError(f"Error retrieving DataFrame: {error}")

    def list_dataframes(
            self,
            prefix: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        List available DataFrames

        Args:
            prefix: Optional prefix to filter DataFrames

        Returns:
            Dict: DataFrame listings

        Raises:
            APIError: If API call fails
        """
        self._refresh_token_if_needed()

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
            raise APIError(f"Error listing DataFrames: {error}")

    def delete_dataframe(
            self,
            dataframe_name: str,
            external_key: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Delete DataFrame from storage

        Args:
            dataframe_name: Name/path of the DataFrame to delete
            external_key: Optional external key to filter what to delete

        Returns:
            Dict: Deletion confirmation

        Raises:
            APIError: If API call fails
        """
        self._refresh_token_if_needed()

        params = {'external_key': external_key} if external_key else {}

        try:
            response = requests.delete(
                f"{self.api_url}/dataframes/{dataframe_name}",
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
            raise APIError(f"Error deleting DataFrame: {error}")
