# models.py - Data models and exceptions

from dataclasses import dataclass
from typing import Dict, Optional, Any, Union, List
import json

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
            'column': self.column,
            'start_timestamp': self.start_timestamp,
            'end_timestamp': self.end_timestamp
        }

@dataclass
class IdFilter:
    column: str
    values: Union[List[Union[int, str]], Union[int, str]] = None
    partition_type: str = 'id'

    def __post_init__(self):
        if self.values is not None and not isinstance(self.values, list):
            self.values = [self.values]
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
    page_size: int = 100
    page_token: Optional[str] = None
