# utils.py - Utility functions

import numpy as np
import pandas as pd
from typing import Dict, Any, Optional


def normalize_dates(df: pd.DataFrame, date_columns: list = ['transaction_date']):
    """
    Normalize date columns to datetime without timezone info

    Args:
        df: Input DataFrame
        date_columns: List of column names containing dates

    Returns:
        DataFrame with normalized dates
    """
    df = df.copy()
    for col in date_columns:
        if col in df.columns:
            # Convert to datetime and remove timezone info
            df[col] = pd.to_datetime(df[col]).dt.tz_localize(None)
    return df
