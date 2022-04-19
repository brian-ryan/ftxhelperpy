import pandas as pd

from datetime import datetime
from dateutil import parser

from ftxhelperpy.utils.connect import Connector

class HistDataFetcher:

    def __init__(self, Connector: type[Connector]):
        self.connector = Connector

    def _format_prices(self, response: dict, include_return: bool) -> pd.DataFrame:
        """Converts the response from a historical prices request to a pandas dataframe

            Args:
                response: The json data of a response object from a historical prices request

            Returns:
                A pandas dataframe with columns for start time, time,
                open, high, low, close, volume
        """

        prices_dict = response['result']
        if len(prices_dict)==0:
            return pd.DataFrame()

        prices_df = pd.DataFrame.from_dict(prices_dict).sort_values(by='time', ascending=True).reset_index(drop=True)
        prices_df['startTime'] = prices_df['startTime'].apply(lambda x: parser.parse(x))

        if include_return:
            prices_df = self._calc_returns(prices_df)

        return prices_df

    def _format_rates(self, response: dict) -> pd.DataFrame:
        """Converts the response from a historical rates request to a pandas dataframe

            Args:
                response: The json data of a response object from a historical funding rates request

            Returns:
                A pandas dataframe with columns for the future, time (datetime) and funding rate
        """

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
                A pandas dataframe of the trade data
        """

        trades_dict = response['result']
        if len(trades_dict)==0:
            return pd.DataFrame()

        trades_df = pd.DataFrame.from_dict(trades_dict).sort_values(by='time', ascending=True).reset_index(drop=True)
        trades_df['time'] = trades_df['time'].apply(lambda x: parser.parse(x))
        return trades_df

    def _calc_returns(self, prices: pd.DataFrame) -> pd.DataFrame:
        """Adds a column for 'return' to a dataframe of
        prices.

            Args:
                prices: A pandas dataframe which must contain
                a column called 'open' representing the price
                of a symbol at the open of an interval

            Returns:
                The prices dataframe with a new column called 'return'
                which is the return from the open of the previous
                interval to the open of the current interval
        """

        prices['return'] = (prices['open']/prices['open'].shift(1)) - 1
        return prices

    def get_future_prices(self, symbol: str, start_time: int,
                              end_time: int, resolution: int = 60,
                          include_return: bool = False) -> pd.DataFrame:
        """Retrieves the historical prices (candle format) for a symbols

            Args:
                symbol: The symbol of the instrument. e.g. BTC-PERP

                start_time: Start of the interval of time to retrieve prices as timestamp

                end_time: End of the interval of time to retrieve prices as timestamp
                resolution: The size of the candle in seconds.
                e.g. resolution = 300 means 5 minute candles

                include_return: Boolean representing whether to calculate
                interval returns on the prices dataframe. The returns are simple returns,
                not log-based i.e. (Pt1/Pt0) - 1 The returns look back, so if the return is 5% at 1/1/2000 12:00 pm,
                and the resolution is set to 60 (1 minute) this means the return from
                11.59 am -> 12:00 pm was 5%. Defaults to false

            Returns:
                A pandas dataframe of historical prices
        """

        endpoint = "markets/{0}/candles".format(symbol)
        query_params = {
            'start_time': start_time,
            'end_time': end_time,
            'resolution': resolution
        }
        response = self.connector.auth_get_request(endpoint, query_params).json()

        if response['success']==False:
            raise Exception(response['error'])

        formatted_prices = self._format_prices(response, include_return)
        if formatted_prices.empty:
            return formatted_prices

        earliest_time = (list(formatted_prices['time'])[0])/ 1000
        # since the api only returns a set number of results
        # recursively call this until we get prices back as early as the start time
        # second condition is just so there is a definite end to the recursion (if for some reason it cannot retrieve
        # results back that far)
        if ((earliest_time - start_time) > (resolution * 2)) and (earliest_time != end_time):
            formatted_prices = pd.concat([self.get_future_prices(symbol, start_time, earliest_time,
                                                                resolution, include_return), formatted_prices])
            formatted_prices = formatted_prices.drop_duplicates(subset=['time']). \
                sort_values(by='time', ascending=True)
            return formatted_prices
        # if the earliest time is close enough to the start time then return
        else:
            return formatted_prices

    def get_index_prices(self, symbol: str, start_time: int,
                              end_time: int, resolution: int = 60,
                            include_return: bool = False) -> pd.DataFrame:
        """Retrieves the historical prices (candle format) for indices

            Args:
                symbol: The symbol of the instrument. e.g. BTC

                start_time: Start of the interval of time to retrieve prices as
                a timestamp

                end_time: End of the interval of time to retrieve prices as a timestamp
                resolution: The size of the candle in seconds.
                e.g. resolution = 300 means 5 minute candles

                include_return: Boolean representing whether to calculate
                interval returns on the prices dataframe. The returns are simple returns,
                not log-based i.e. (Pt1/Pt0) - 1 The returns look back, so if the return is 5% at 1/1/2000 12:00 pm,
                and the resolution is set to 60 (1 minute) this means the return from
                11.59 am -> 12:00 pm was 5%. Defaults to false

            Returns:
                A pandas dataframe of historical prices
        """

        endpoint = "indexes/{0}/candles".format(symbol)
        query_params = {
            'start_time': start_time,
            'end_time': end_time,
            'resolution': resolution
        }
        response = self.connector.auth_get_request(endpoint, query_params).json()
        if response['success']==False:
            raise Exception(response['error'])

        formatted_prices = self._format_prices(response, include_return)
        if formatted_prices.empty:
            return formatted_prices

        earliest_time = (list(formatted_prices['time'])[0])/ 1000
        # since the api only returns a set number of results
        # recursively call this until we get prices back as early as the start time
        if ((earliest_time - start_time) > (resolution * 2)) & (earliest_time != end_time):
            formatted_prices =  pd.concat([self.get_index_prices(symbol, start_time, earliest_time,
                                                                resolution, include_return), formatted_prices])
            formatted_prices = formatted_prices.drop_duplicates(subset = ['time']).\
                sort_values(by = 'time', ascending = True)
            return formatted_prices
        else:
            return formatted_prices

    def get_rates(self, symbol: str, start_time: int, end_time: int) -> pd.DataFrame:
        """Retrieves the historical prices (candle format) for a symbol

            Args:
                symbol: The symbol of the instrument. e.g. BTC-PERP
                start_time: Start of the interval of time to retrieve rates as timestamp
                end_time: End of the interval of time to retrieve rates as timestamp

            Returns:
                A pandas data frame with columns for the future name,
                the funding rate, and the time.

                - The funding rate is in decimal format.
                - The funding rate is 1/24 of the premium of the future
                to the underlying based on the previous 1 hour TWAP for the
                swap and the future.
        """

        endpoint = "funding_rates"
        query_params = {
            'future': symbol,
            'start_time': start_time,
            'end_time': end_time,
        }

        response = self.connector.auth_get_request(endpoint, query_params).json()

        if response['success']==False:
            raise Exception(response['error'])

        formatted_rates = self._format_rates(response)

        if formatted_rates.empty:
            return formatted_rates

        earliest_time = list(formatted_rates['time'])[0].timestamp()
        # since the api only returns a set number of results
        # recursively call this until we get prices back as early as the start time
        if ((earliest_time - start_time) > (60*60)) & (earliest_time != end_time):
            formatted_rates = pd.concat([self.get_rates(symbol, start_time, earliest_time), formatted_rates])
            formatted_rates = formatted_rates.drop_duplicates(subset=['time']). \
                sort_values(by='time', ascending=True)
            return formatted_rates
        else:
            return formatted_rates

    def get_trades(self, symbol: str, start_time: int, end_time: int) -> pd.DataFrame:
        """Retrieves the historical trades for a given symbol
        This endpoint does not guarantee it will retrieve results for the
        full timeframe as FTX only returns a set number of results in
        its response.

            Args:
                symbol: The symbol of the instrument. e.g. BTC-PERP
                start_time: Start of the interval of time to retrieve trades as timestamp
                end_time: End of the interval of time to retrieve trades as timestamp

            Returns:
                A pandas dataframe with columns for the trade id,
                price, size, side, liquidation (boolean if the trade involved
                a liquidation order), and time
        """

        endpoint = "/markets/{0}/trades".format(symbol)
        query_params = {
            'market_name': symbol,
            'start_time': start_time,
            'end_time': end_time
        }

        response = self.connector.auth_get_request(endpoint, query_params).json()

        if response['success']==False:
            raise Exception(response['error'])

        formatted_trades =  self._format_trades(response)
        return formatted_trades

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