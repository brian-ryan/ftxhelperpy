import pandas as pd

from datetime import datetime
from dateutil import parser

from ftxhelperpy.utils.connect import Connector

class HistDataFetcher:

    def __init__(self, Connector: type[Connector]):
        self.connector = Connector

    def _format_prices(self, response: dict) -> pd.DataFrame:
        """Converts the response from a historical prices request to a pandas dataframe

            Args:
                response: The json data of a response object from a historical prices request

            Returns:
                A pandas dataframe with columns for start time, time,
                open, high, low, close, volume"""

        prices_dict = response['result']
        if len(prices_dict)==0:
            return pd.DataFrame()

        prices_df = pd.DataFrame.from_dict(prices_dict).sort_values(by='time', ascending=True).reset_index(drop=True)
        prices_df['startTime'] = prices_df['startTime'].apply(lambda x: parser.parse(x))
        return prices_df

    def _format_rates(self, response: dict) -> pd.DataFrame:
        """Converts the response from a historical rates request to a pandas dataframe

            Args:
                response: The json data of a response object from a historical funding rates request

            Returns:
                A pandas dataframe with columns for the future, time (datetime) and funding rate"""

        rates_dict = response['result']
        if len(rates_dict)==0:
            return pd.DataFrame()

        rates_df = pd.DataFrame.from_dict(rates_dict).sort_values(by='time', ascending=True).reset_index(drop=True)
        rates_df['time'] = rates_df['time'].apply(lambda x: parser.parse(x))
        return rates_df

    def _format_trades(self, response: dict) -> pd.DataFrame:
        """Converts the response from a historical trades request to a pandas dataframe
            Args:
                response: The json data of a response object from a historical funding rates request

            Returns:
                A pandas dataframe of the trade data"""

        trades_dict = response['result']
        if len(trades_dict)==0:
            return pd.DataFrame()

        trades_df = pd.DataFrame.from_dict(trades_dict).sort_values(by='time', ascending=True).reset_index(drop=True)
        trades_df['time'] = trades_df['time'].apply(lambda x: parser.parse(x))
        return trades_df

    def get_future_prices(self, symbol: str, start_time: datetime,
                              end_time: datetime, resolution: int = 60) -> pd.DataFrame:
        """Retrieves the historical prices (candle format) for a symbols

            Args:
                symbol: The symbol of the instrument. e.g. BTC-PERP
                start_time: Start of the interval of time to retrieve prices
                end_time: End of the interval of time to retrieve prices
                resolution: The size of the candle in seconds.
                e.g. resolution = 300 means 5 minute candles

            Returns:
                A pandas dataframe of historical prices"""

        endpoint = "markets/{0}/candles".format(symbol)
        query_params = {
            'start_time': datetime.timestamp(start_time),
            'end_time': datetime.timestamp(end_time),
            'resolution': resolution
        }
        response = self.connector.auth_get_request(endpoint, query_params).json()

        if response['success']==False:
            raise Exception(response['error'])

        return self._format_prices(response)

    def get_index_prices(self, symbol: str, start_time: datetime,
                              end_time: datetime, resolution: int = 60) -> pd.DataFrame:
        """Retrieves the historical prices (candle format) for indices

            Args:
                symbol: The symbol of the instrument. e.g. BTC
                start_time: Start of the interval of time to retrieve prices
                end_time: End of the interval of time to retrieve prices
                resolution: The size of the candle in seconds.
                e.g. resolution = 300 means 5 minute candles

            Returns:
                A pandas dataframe of historical prices"""

        endpoint = "indexes/{0}/candles".format(symbol)
        query_params = {
            'start_time': datetime.timestamp(start_time),
            'end_time': datetime.timestamp(end_time),
            'resolution': resolution
        }
        response = self.connector.auth_get_request(endpoint, query_params).json()
        if response['success']==False:
            raise Exception(response['error'])

        return self._format_prices(response)

    def get_rates(self, symbol: str, start_time: datetime, end_time: datetime) -> pd.DataFrame:
        """Retrieves the historical prices (candle format) for a symbol

            Args:
                symbol: The symbol of the instrument. e.g. BTC-PERP
                start_time: Start of the interval of time to retrieve rates
                end_time: End of the interval of time to retrieve rates

            Returns:
                A pandas data frame with columns for the future name,
                the funding rate, and the time.

                - The funding rate is in decimal format.
                - The funding rate is 1/24 of the premium of the future
                to the underlying based on the previous 1 hour TWAP for the
                swap and the future."""

        endpoint = "funding_rates"
        query_params = {
            'future': symbol,
            'start_time': datetime.timestamp(start_time),
            'end_time': datetime.timestamp(end_time),
        }

        response = self.connector.auth_get_request(endpoint, query_params).json()

        if response['success']==False:
            raise Exception(response['error'])

        return self._format_rates(response)

    def get_trades(self, symbol: str, start_time: datetime, end_time: datetime) -> pd.DataFrame:
        """Retrieves the historical trades for a given symbol

            Args:
                symbol: The symbol of the instrument. e.g. BTC-PERP
                start_time: Start of the interval of time to retrieve trades
                end_time: End of the interval of time to retrieve trades

            Returns:
                A pandas dataframe with columns for the trade id,
                price, size, side, liquidation (boolean if the trade involved
                a liquidation order), and time
        """

        endpoint = "/markets/{0}/trades".format(symbol)
        query_params = {
            'market_name': symbol,
            'start_time': datetime.timestamp(start_time),
            'end_time':datetime.timestamp(end_time)
        }

        response = self.connector.auth_get_request(endpoint, query_params).json()

        if response['success']==False:
            raise Exception(response['error'])

        return self._format_trades(response)

class LiveDataFetcher:

    def __init__(self, Connector: type[Connector]):
        self.connector = Connector

    def get_order_book(self, symbol: str, depth: int = 20) -> dict:
        """Returns the order book for the given symbol

            Args:
                symbol: A string representing the
                symbol to retrieve the order book for

                depth: How many levels of the bid/offer
                to retrieve. Defaults to 20, max value is 100

            Returns:
                 A dictionary with top level keys consisting
                 of 'bids' and 'asks'. Each of these keys
                 contains an array of objects where each object
                 has a key for 'price' and 'size' indicating
                 a level. The first element in each array
                 is the most competitive bid/ask
            """

        endpoint = "/markets/{0}/orderbook".format(symbol)
        query_params = {
            'depth': depth
        }

        response = self.connector.auth_get_request(endpoint, query_params).json()

        if response['success'] == False:
            raise Exception(response['error'])

        order_book = {}
        order_book['bids'] = list(map(lambda x: {'price': x[0], 'size': x[1]}, response['result']['bids']))
        order_book['asks'] = list(map(lambda x: {'price': x[0], 'size': x[1]}, response['result']['asks']))
        return order_book
