# utils.py - Utility functions

import numpy as np
import pandas as pd
from typing import Dict, Any, Optional


def convert_numpy_types(value):
    """Convert numpy types to native Python types"""
    if isinstance(value, (np.int_, np.intc, np.intp, np.int8,
                          np.int16, np.int32, np.int64, np.uint8,
                          np.uint16, np.uint32, np.uint64)):
        return int(value)
    elif isinstance(value, (np.float_, np.float16, np.float32, np.float64)):
        return float(value)
    elif isinstance(value, (np.bool_)):
        return bool(value)
    elif isinstance(value, (np.ndarray,)):
        return value.tolist()
    return value


def prepare_dataframe_for_upload(df: pd.DataFrame, columns_keys: Optional[Dict[str, str]] = None) -> pd.DataFrame:
    """Prepare DataFrame for upload by converting data types"""
    df_copy = df.copy()

    if columns_keys:
        for col, key_type in columns_keys.items():
            if key_type == 'Date':
                df_copy[col] = pd.to_datetime(df_copy[col]).dt.strftime('%Y-%m-%d')

    # Convert numeric columns to Python native types
    for col in df_copy.select_dtypes(include=['integer', 'floating']).columns:
        df_copy[col] = df_copy[col].map(convert_numpy_types)

    return df_copy
