# auth.py - Authentication handling
import boto3
import requests
from typing import Dict, Optional
from .models import AuthenticationError, APIError


class AuthManager:
    def __init__(self, api_url: str, region: str = 'eu-west-1'):
        self.api_url = api_url.rstrip('/')
        self.region = region

    def get_auth_token(self, user: str, password: str) -> str:
        try:
            response = requests.get(f"{self.api_url}/auth/config")
            response.raise_for_status()
            client_id = response.json()['userPoolClientId']

            cognito = boto3.client('cognito-idp', region_name=self.region)
            auth = cognito.initiate_auth(
                ClientId=client_id,
                AuthFlow='USER_PASSWORD_AUTH',
                AuthParameters={'USERNAME': user, 'PASSWORD': password}
            )
            return auth['AuthenticationResult']['IdToken']
        except Exception as e:
            raise AuthenticationError(f"Authentication failed: {str(e)}")

    def verify_token(self, headers: Dict[str, str]) -> bool:
        try:
            response = requests.get(f"{self.api_url}/auth/verify", headers=headers)
            return response.status_code == 200
        except requests.exceptions.RequestException:
            return False
