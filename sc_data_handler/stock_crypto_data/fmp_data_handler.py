import os
import datetime
from typing import Dict, List, Tuple, Union
from urllib.request import urlopen
from urllib.parse import urlencode
import certifi
import json
import pandas as pd
import pytz

FMP_API_KEY = os.environ.get("FMP_API_KEY", "xJNks2ZWMQAos8g6TlwXBQBJj73WuCkX")
est = pytz.timezone("US/Eastern")
utc = pytz.utc


def parse_date_string(date_string: str) -> datetime.datetime:
    """Parses a date string to a datetime object

    Raises:
        ValueError: If the date string cannot be parsed.

    Returns:
        datetime.datetime: The datetime object
    """
    possible_formats = ["%Y-%m-%d %H:%M:%S%z", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d"]
    for date_format in possible_formats:
        try:
            datetime_object = datetime.datetime.strptime(date_string, date_format)
            return datetime_object
        except ValueError:
            pass
    # If no format matches, you might want to handle this case accordingly
    raise ValueError(
        "No matching date format found, must be either %Y-%m-%d %H:%M:%S%z, %Y-%m-%d %H:%M:%S or %Y-%m-%d"
    )


def get_jsonparsed_data(url):
    response = urlopen(url, cafile=certifi.where())
    data = response.read().decode("utf-8")
    return json.loads(data)


class FMPStockCryptoDataRetriever:
    """A class handler for retrieving stock, etf, and crypto data from Financial Modeling Prep"""

    def __init__(self, api_key: str = FMP_API_KEY):
        self.api_key = api_key
        self.base_api_url = "https://financialmodelingprep.com/api/v3"
        self.stock_list_url = f"{self.base_api_url}/stock/list"
        self.etf_list_url = f"{self.base_api_url}/etf/list"
        self.crypto_list_url = f"{self.base_api_url}/symbol/available-cryptocurrencies"

        self.intraday_url = f"{self.base_api_url}/historical-chart"
        self.daily_url = f"{self.base_api_url}/historical-price-full"

        self.active_symbols = (
            self.get_data_list_short("stock")
            + self.get_data_list_short("crypto")
            + self.get_data_list_short("etf")
        )

    def get_data_list_full(self, data_type: str = "stock") -> List[Dict]:
        """Retrieve a list of all the stocks, eft, or crypto available on Financial Modeling Prep

        Args:
            data_type (str, optional): The type of data to retrieve. Options are 'stock', 'etf', 'crypto'. Defaults to 'stock'.

        Returns:
            List[Dict]: A list of dictionaries containing the stock data,
            including the symbol, name, price, and exchange information
        """
        if data_type == "stock":
            url = f"{self.stock_list_url}?apikey={self.api_key}"
        elif data_type == "etf":
            url = f"{self.etf_list_url}?apikey={self.api_key}"
        elif data_type == "crypto":
            url = f"{self.crypto_list_url}?apikey={self.api_key}"
        else:
            raise Exception(
                "Invalid data_type value. Must be 'stock', 'etf', or 'crypto'"
            )
        return get_jsonparsed_data(url)

    def get_data_list_short(self, data_type: str = "stock") -> List[str]:
        """Retrieve a list of all the stocks, eft, or crypto available on Financial Modeling Prep

        Args:
            data_type (str, optional): The type of data to retrieve. Options are 'stock', 'etf', 'crypto'. Defaults to 'stock'.

        Returns:
            List[str]: A list of the symbols names
        """
        data_list = self.get_data_list_full(data_type=data_type)
        return [data["symbol"].lower() for data in data_list]

    def validate_asset_symbol(self, symbol: str) -> Tuple[bool, str]:
        """verify that the symbol is an active/available symbol

        Args:
            symbol (str): the symbol to validate

        Returns:
            Tuple[bool, str]: A tuple containing a boolean value indicating if the symbol is valid and the type of the symbol
        """
        stock_list = self.get_data_list_short("stock")
        if symbol.lower() in stock_list:
            return True, "stock"
        etf_list = self.get_data_list_short("etf")
        if symbol.lower() in etf_list:
            return True, "stock"
        crypto_list = self.get_data_list_short("crypto")
        if symbol.lower() in crypto_list:
            return True, "crypto"
        return False, None

    def retrieve_historical_bars(
        self,
        asset_symbol: str,
        start_date: datetime.datetime,
        end_date: datetime.datetime,
        tframe: str,
        type_: str,
        format_taLib: bool = False,
    ) -> Union[List[Dict], Dict]:
        """Retrieve historical stock, crypto, or etf bars using the Financial Modeling Prep API

        Args:
            asset_symbol (str): the stock/crypto/etf symbol to retrieve data for
            start_date (str): the date to begin retrieving data from
            end_date (str):  the date to stop retrieving data
            tframe (str): the time frame for the retrieved data, can be '1min', '5min', '15min', '30min', '1hour', '4hour', '1day', '1week', '1month'
            type_ (str): the type of asset to retrieve data for, can be 'stock', or 'crypto'. 'eft' is covered under 'stock'
            format_taLib (bool, optional): If True, the data will be formatted for use with the taLib library. Defaults to False.

        Returns:
            Union[List[Dict], Dict]: A list of dictionaries containing the historical data for the asset, returns a dictionary if format_taLib is True
        """

        if tframe not in ["1min", "5min", "15min", "30min", "1hour", "4hour", "1day"]:
            raise Exception(
                "Invalid timeframe value. Must be '1min', '5min', '15min', '30min', '1hour', '4hour', or '1day'"
            )
        if type_ not in ["stock", "crypto"]:
            raise Exception("Invalid type value. Must be 'stock', 'crypto', or 'etf'")

        url_params = {
            "apikey": self.api_key,
            "from": start_date.strftime("%Y-%m-%d"),
            "to": end_date.strftime("%Y-%m-%d"),
            "extended": True,
        }

        if tframe == "1day":
            url = f"{self.daily_url}/{asset_symbol}?{urlencode(url_params)}"
            time_format = "%Y-%m-%d"
        else:
            url = f"{self.intraday_url}/{tframe}/{asset_symbol}?{urlencode(url_params)}"
            time_format = "%Y-%m-%d %H:%M:%S"

        print(f"\n\nRetrieving data from {url}\n\n")
        data = get_jsonparsed_data(url)
        # The datas retrieved is in descending order
        # Historical key is present for 1day data, else it is not, Also limit the data to the last 10k points
        if isinstance(data, dict):
            data = data.get("historical", [])
        if not data:
            return []

        # reverse the order, it is currently in ascending order, we want descending order
        data = data[::-1]

        if format_taLib:
            new_data = {
                "open": [],
                "high": [],
                "low": [],
                "close": [],
                "volume": [],
                "date": [],
            }

            for bar in data:
                new_data["open"].append(bar["open"])
                new_data["high"].append(bar["high"])
                new_data["low"].append(bar["low"])
                new_data["close"].append(bar["close"])
                new_data["volume"].append(bar["volume"])
                new_data["date"].append(bar["date"])
            return new_data

        data = data[-10000:]
        # make timestamp from EST to UTC, note that timestamp is a string, not as a dataframe
        data = [
            {
                "datetime": est.localize(
                    datetime.datetime.strptime(bar["date"], time_format)
                )
                .astimezone(utc)
                .strftime("%Y-%m-%d %H:%M:%S%z"),
                "open": bar["open"],
                "high": bar["high"],
                "low": bar["low"],
                "close": bar["close"],
                "volume": bar["volume"],
            }
            for bar in data
        ]

        return data
