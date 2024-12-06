from io import StringIO
import pandas as pd
import boto3
import requests
from requests.exceptions import RequestException
from typing import Dict, Optional, Any
import json
import time
import math
import logging

from .models import (
    DataFrameClientError,
    S3SelectClientError,
    AuthenticationError,
    APIError,
)
from .auth import AuthManager
from .utils import convert_numpy_types

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


class DataFrameClient:

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
            query: Optional[str] = None,
            retries: int = 3,
            retry_delay: int = 1,
            timeout: int = 30,
            chunk_size: Optional[int] = None
    ) -> pd.DataFrame:
        """
        Query S3 data using S3 Select and return as DataFrame

        Args:
            dataframe_name: S3 key/path of the source file
            query: Optional S3 Select SQL query (default: SELECT * FROM s3object)
            retries: Number of download retries
            retry_delay: Seconds to wait between retries
            timeout: Request timeout in seconds
            chunk_size: Optional chunk size for reading large files

        Returns:
            pandas.DataFrame with query results
        """
        try:
            self._refresh_token_if_needed()

            # Prepare query parameters
            params = {}
            if query:
                params['query'] = query
                logger.info(f"Query: {query} for {dataframe_name}")

            # URL encode the key for path parameters while preserving slashes
            encoded_key = requests.utils.quote(dataframe_name, safe='')

            # Get pre-signed URL with retries
            for attempt in range(retries):
                try:
                    response = requests.get(
                        f"{self.api_url}/dataframes/{encoded_key}/get",
                        params=params,
                        headers=self.headers,  # Include authorization headers
                        timeout=timeout
                    )
                    response.raise_for_status()
                    data = response.json()
                    download_url = data['download_url']
                    logger.info("Successfully obtained pre-signed URL")
                    break
                except requests.exceptions.RequestException as e:
                    if attempt == retries - 1:
                        raise APIError(f"Failed to get pre-signed URL: {str(e)}")
                    logger.warning(f"Attempt {attempt + 1} failed, retrying...")
                    time.sleep(retry_delay * (2 ** attempt))

            # Download data with retries
            for attempt in range(retries):
                try:
                    if chunk_size:
                        # Stream large files
                        chunks = []
                        with requests.get(download_url, stream=True, timeout=timeout) as r:
                            r.raise_for_status()
                            for chunk in pd.read_csv(r.raw, chunksize=chunk_size):
                                chunks.append(chunk)
                        df = pd.concat(chunks, ignore_index=True)
                    else:
                        # Small files
                        df = pd.read_csv(download_url, timeout=timeout)

                    logger.info(f"Successfully downloaded data: {len(df)} rows")
                    return df

                except Exception as e:
                    if attempt == retries - 1:
                        raise APIError(f"Failed to download data: {str(e)}")
                    logger.warning(f"Download attempt {attempt + 1} failed, retrying...")
                    time.sleep(retry_delay * (2 ** attempt))

        except Exception as e:
            logger.error(f"Error in get_dataframe: {str(e)}")
            raise

    def post_dataframe(
            self,
            df: pd.DataFrame,
            dataframe_name: str,
            chunk_size: int = 5 * 1024 * 1024,  # 5MB chunks
            retries: int = 3,
            retry_delay: int = 1,
            timeout: int = 300
    ) -> Dict[str, Any]:
        """
        Upload a DataFrame to S3 through the API using multipart upload

        Args:
            df: pandas DataFrame to upload
            dataframe_name: S3 dataframe_name/dataframe_id for the uploaded file
            chunk_size: Size of upload chunks in bytes (default: 5MB)
            retries: Number of upload retries
            retry_delay: Seconds to wait between retries
            timeout: Request timeout in seconds

        Returns:
            Dict with upload results containing dataframe_name info
        """
        try:
            self._refresh_token_if_needed()

            if df.empty:
                raise DataFrameClientError("DataFrame is empty")

            # URL encode the key for path parameters while preserving slashes
            encoded_key = requests.utils.quote(dataframe_name, safe='')

            url = f"{self.api_url}/dataframes/{encoded_key}/upload"

            # Initialize multipart upload
            response = self._make_request(
                'POST',
                url,
                timeout=timeout,
                retries=retries,
                retry_delay=retry_delay
            )
            upload_id = response['upload_id']

            # Convert DataFrame to CSV data
            csv_buffer = StringIO()
            df.to_csv(csv_buffer, index=False)
            csv_buffer.seek(0)
            csv_data = csv_buffer.getvalue().encode('utf-8')

            # Calculate parts
            total_size = len(csv_data)
            parts = []

            # Upload parts
            for part_number in range(1, math.ceil(total_size / chunk_size) + 1):
                complete_response = self._make_request(
                    'POST',  # Note: Changed from POST to PUT to match server
                    url,
                    json={
                        'part_number': part_number,
                        'upload_id': upload_id,
                    },
                    timeout=timeout,
                    retries=retries,
                    retry_delay=retry_delay
                )
                part_url = complete_response['upload_url']

                # Upload the part
                start_pos = (part_number - 1) * chunk_size
                end_pos = min(start_pos + chunk_size, total_size)
                part_data = csv_data[start_pos:end_pos]

                for attempt in range(retries):
                    try:
                        part_response = requests.put(
                            part_url,
                            data=part_data,
                            timeout=timeout
                        )
                        part_response.raise_for_status()

                        parts.append({
                            'PartNumber': part_number,
                            'ETag': part_response.headers['ETag']
                        })

                        logger.info(f"Uploaded part {part_number}, size: {len(part_data)} bytes")
                        break

                    except requests.exceptions.RequestException as e:
                        if attempt == retries - 1:
                            raise APIError(f"Failed to upload part {part_number}: {str(e)}")
                        time.sleep(retry_delay * (2 ** attempt))

            # Complete multipart upload
            complete_response = self._make_request(
                'POST',  # Note: Changed from POST to PUT to match server
                url,
                json={
                    'parts': parts,
                    'upload_id': upload_id,
                },
                timeout=timeout,
                retries=retries,
                retry_delay=retry_delay
            )

            return complete_response

        except Exception as e:
            if isinstance(e, (DataFrameClientError, APIError, AuthenticationError)):
                raise
            raise DataFrameClientError(f"Error in post dataframe: {str(e)}")

    def _make_request(
            self,
            method: str,
            url: str,
            retries: int,
            retry_delay: int,
            timeout: int,
            **kwargs
    ) -> Dict[str, Any]:
        """Helper method to make HTTP requests with retries"""
        for attempt in range(retries):
            try:
                response = requests.request(
                    method,
                    url,
                    headers=self.headers,
                    timeout=timeout,
                    **kwargs
                )
                response.raise_for_status()
                return response.json()

            except requests.exceptions.RequestException as e:
                if attempt == retries - 1:
                    raise APIError(f"Request failed after {retries} attempts: {str(e)}")
                time.sleep(retry_delay * (2 ** attempt))
