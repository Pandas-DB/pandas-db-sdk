# models.py - Data models and exceptions

class DataFrameClientError(Exception): pass
class AuthenticationError(DataFrameClientError): pass
class APIError(DataFrameClientError): pass
class S3SelectClientError(Exception): pass