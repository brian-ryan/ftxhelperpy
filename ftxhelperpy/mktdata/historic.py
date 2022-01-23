import pandas as pd

from datetime import datetime, timedelta

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

        prices_df = pd.DataFrame.from_dict(prices_dict)
        return prices_df

    def get_historical_prices(self, symbol: str, start_time: datetime,
                              end_time: datetime, resolution: int = 60) -> pd.DataFrame():
        """Retrieves the historical prices (candle format) for a symbols

            Args:
                symbol: The symbol of the instrument. e.g. BTC-PERP
                start_time: Start of the interval of time to retrieve prices
                end_time: End of the interval of time to retrieve prices
                resolution: The size of the candle in seconds.
                e.g. resolution = 300 means 5 minute candles"""

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
