from concurrent.futures import ThreadPoolExecutor, as_completed
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


def parse_date_string(
    date_string: str, get_current_time: bool = False
) -> Tuple[datetime.datetime, Union[datetime.datetime, None]]:
    """Parses a date string to a datetime object

    Args:
        date_string (str): The date string to parse
        get_current_time_string (bool, optional): If True, the current time string will be returned in the same format. Defaults to False.

    Raises:
        ValueError: If the date string cannot be parsed.

    Returns:
        datetime.datetime: The parsed datetime object
    """
    possible_formats = ["%Y-%m-%d %H:%M:%S%z", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d"]
    for date_format in possible_formats:
        try:
            datetime_object = datetime.datetime.strptime(date_string, date_format)
            current_date_time = None
            if get_current_time:
                # make sure is same format and timezone as datetime_object
                current_date_time = datetime.datetime.now(datetime_object.tzinfo)

            return datetime_object, current_date_time
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


def minute_to_count_in_market_day(min_, max_range):
    six_hour_thirty_mins = 390
    return int(max_range * six_hour_thirty_mins / min_)


def minute_to_count_in_day(min_, max_range):
    twenty_four_hours = 1440
    return int(max_range * twenty_four_hours / min_)


class FMPStockCryptoDataRetriever:
    """A class handler for retrieving stock, etf, and crypto data from Financial Modeling Prep"""

    # The maximum number of days of data that can be retrieved for a particular time frame.
    # If you require more than [X]days of [X]-minute data, you will need to implement a loop
    # that iterates over [X]-day intervals covering the entire desired time range.
    interval_to_max_days: Dict[str, Dict[str, int]] = {
        "1min": {
            "max_range_days": 3,
            # the number of data points to expect over max_range_days. Only accounts for market hours 9:30 - 4
            "num_points_market_hours_only": minute_to_count_in_market_day(1, 3),
            "num_points": minute_to_count_in_day(1, 3),
        },
        "5min": {
            "max_range_days": 10,
            "num_points_market_hours_only": minute_to_count_in_market_day(5, 10),
            "num_points": minute_to_count_in_day(5, 10),
        },
        "15min": {
            "max_range_days": 45,
            "num_points_market_hours_only": minute_to_count_in_market_day(15, 45),
            "num_points": minute_to_count_in_day(15, 45),
        },
        "30min": {
            "max_range_days": 30,
            "num_points_market_hours_only": minute_to_count_in_market_day(30, 30),
            "num_points": minute_to_count_in_day(30, 30),
        },
        "1hour": {
            "max_range_days": 90,
            "num_points_market_hours_only": minute_to_count_in_market_day(60, 90),
            "num_points": minute_to_count_in_day(60, 90),
        },
        "4hour": {
            "max_range_days": 180,
            "num_points_market_hours_only": minute_to_count_in_market_day(240, 180),
            "num_points": minute_to_count_in_day(240, 180),
        },
        "1day": {
            "max_range_days": 1825,
            "num_points_market_hours_only": 1825,
            "num_points": 1825,
        },
        "1week": {
            "max_range_days": 14600,
            "num_points_market_hours_only": 14600 / 7,
            "num_points": 14600 / 7,
        },
        "1month": {
            "max_range_days": 14600,
            "num_points_market_hours_only": 14600 / 30,
            "num_points": 14600 / 30,
        },
        "1year": {
            "max_range_days": 14600,
            "num_points_market_hours_only": 14600 / 365,
            "num_points": 14600 / 365,
        },
    }

    timeframes = [
        "1min",
        "5min",
        "15min",
        "30min",
        "45min",
        "1hour",
        "4hour",
        "1day",
        "1week",
        "1month",
        "1year",
    ]

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
            tframe (str): the time frame for the retrieved data, can be '1min', '5min', '15min', '30min', '45min', '1hour', '4hour', '1day', '1week', '1month', '1year'
            type_ (str): the type of asset to retrieve data for, can be 'stock', or 'crypto'. 'eft' is covered under 'stock'
            format_taLib (bool, optional): If True, the data will be formatted for use with the taLib library. Defaults to False.

        Returns:
            Union[List[Dict], Dict]: A list of dictionaries containing the historical data for the asset, returns a dictionary if format_taLib is True
        """

        if tframe not in self.timeframes:
            raise Exception(
                f"Invalid timeframe value. Must be one of {', '.join(self.timeframes)}"
            )
        if type_ not in ["stock", "crypto"]:
            raise Exception("Invalid type value. Must be 'stock', or 'crypto'")

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

    def recurssive_call_retrieve_historical_bars(
        self,
        asset_symbol: str,
        start_date: datetime.datetime,
        end_date: datetime.datetime,
        tframe: str,
        type_: str,
        format_taLib: bool = False,
    ) -> Union[List[Dict], Dict]:
        """FMP has a limit on the number of days of data that can be retrieved at once for a particular time frame.
        This method will make multiple parrallel calls to retrieve the data in chunks, up until 10k data points total
        are retrieved, or the start_date is reached - whichever comes first. We work from the end_date backwards

        Args:
            asset_symbol (str): the stock/crypto/etf symbol to retrieve data for
            start_date (str): the date to begin retrieving data from
            end_date (str):  the date to stop retrieving data
            tframe (str): the time frame for the retrieved data, can be '1min', '5min', '15min', '30min', '45min', '1hour', '4hour', '1day', '1week', '1month', '1year'
            type_ (str): the type of asset to retrieve data for, can be 'stock', or 'crypto'. 'eft' is covered under 'stock'
            format_taLib (bool, optional): If True, the data will be formatted for use with the taLib library. Defaults to False.

        Returns:
            Union[List[Dict], Dict]: A list of dictionaries containing the historical data for the asset, returns a dictionary if format_taLib is True
        """

        max_days = self.interval_to_max_days.get(tframe, {}).get("max_range_days", 0)

        if max_days == 0:
            raise Exception("Invalid timeframe value")

        if type_ not in ["stock", "crypto"]:
            raise Exception("Invalid type value. Must be 'stock' or 'crypto'")

        if type_ == "stock":
            total_num_points = self.interval_to_max_days.get(tframe, {}).get(
                "num_points_market_hours_only", 0
            )
        else:
            total_num_points = self.interval_to_max_days.get(tframe, {}).get(
                "num_points", 0
            )

        print(
            f"\n\nRetrieving data for {asset_symbol} from {start_date} to {end_date} with time frame {tframe}, with total_num_points {total_num_points} and max_days {max_days}"
        )
        # get a new start date, max days behind the end date. Do this until the start date is reached
        start_end_date_pairs = []
        total_points = 0

        # retrieve a max of 20k points to roughly account for weekends and stuff, will filter out the last 10k
        while end_date >= start_date and total_points < 20000:
            new_start_date = end_date - datetime.timedelta(days=max_days - 1)
            start_end_date_pairs.append((new_start_date, end_date))
            end_date = new_start_date - datetime.timedelta(days=1)
            total_points += total_num_points

        # call retrieve_historical_bars in parrallel for each start_end_date_pair
        data = []
        num_datas = []
        with ThreadPoolExecutor(max_workers=10) as executor:
            results = [
                executor.submit(
                    self.retrieve_historical_bars,
                    asset_symbol,
                    start_date_i,
                    end_date_i,
                    tframe,
                    type_,
                    format_taLib,
                )
                for start_date_i, end_date_i in start_end_date_pairs
            ]
            for f in as_completed(results):
                data.extend(f.result())
                num_datas.append(len(f.result()))
        print(
            f"\n\nRetrieved data for {asset_symbol} from {start_date} to {end_date} with time frame {tframe}, with total_num_points {total_num_points}, data length: {len(data)} and max_days {max_days}, iterating over {len(start_end_date_pairs)} pairs: {start_end_date_pairs}, num_datas: {num_datas}"
        )
        data.sort(key=lambda x: x["datetime"])
        data = data[-10000:]
        return data
