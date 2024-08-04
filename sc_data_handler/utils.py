from datetime import date, datetime
from typing import List, Union
import numpy as np
import pandas as pd
import json
from decimal import Decimal


class GeneralEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, np.ndarray):
            return np.where(np.isnan(obj), None, obj).tolist()
        if isinstance(obj, pd.Timestamp):
            return obj.strftime("%Y-%m-%d %H:%M:%S%z")
        if isinstance(obj, np.int64):
            return int(obj)
        if isinstance(obj, (float, np.float32, np.float64)):
            if np.isnan(obj):
                return None
            else:
                return float(obj)
        if isinstance(obj, datetime):
            return obj.strftime("%Y-%m-%d %H:%M:%S%z")
        if isinstance(obj, date):
            return obj.strftime("%Y-%m-%d %H:%M:%S%z")
        if isinstance(obj, pd.DataFrame):
            # Convert DataFrame to dictionary
            return obj.to_dict(orient="records")
        if isinstance(obj, pd.Series):
            return obj.to_dict()
        if isinstance(obj, Decimal):
            return str(obj)
        # if isinstance(obj, list):
        #     return f"{[self.default(item) for item in obj]}"
        return super(GeneralEncoder, self).default(obj)


def get_last_n_points(data: Union[np.ndarray, List], n: int = 10000) -> np.ndarray:
    isList = isinstance(data, List)

    if not isinstance(data, np.ndarray) and not isList:
        raise ValueError("Input data must be a 1D or 2D numpy ndarray")

    if isList:
        data = np.array(data)

    if data.ndim == 1:
        # 1D array, return the last 10,000 points
        return data[-n:]
    elif data.ndim == 2:
        # 2D array, return the last 10,000 points of each array
        return data[:, -n:]
