import pandas as pd

from ftxhelperpy.utils.connect import Connector

class MetaDataFetcher:

    def __init__(self, Connector: type[Connector]):
        self.connector = Connector

    def get_all_futures(self, sort_by: str ='volumeUsd24h') -> pd.DataFrame:
        """Returns a pandas dataframe of all currently active futures.

            Args:
                sort_by: Column name to sort by in descending order (optional)

            Returns:
                A pandas dataframe with columns (at time of writing) for
                ['name', 'underlying', 'description', 'type', 'expiry', 'perpetual',
               'expired', 'enabled', 'postOnly', 'priceIncrement', 'sizeIncrement',
               'last', 'bid', 'ask', 'index', 'mark', 'imfFactor', 'lowerBound',
               'upperBound', 'underlyingDescription', 'expiryDescription', 'moveStart',
               'marginPrice', 'positionLimitWeight', 'group', 'change1h', 'change24h',
               'changeBod', 'volumeUsd24h', 'volume', 'openInterest',
               'openInterestUsd']"""

        endpoint = "futures"
        response = self.connector.auth_get_request(endpoint).json()

        if response['success']==False:
            raise Exception(response['error'])

        futures_df = pd.DataFrame.from_dict(response['result']).sort_values(by=sort_by, ascending=False).reset_index(drop=True)
        return futures_df

    def get_future(self, fut_symbol: str) -> dict:
        """Returns a dictionary representing a single future.

            Args:
                fut_symbol: The symbol of the future to retrieve

            Returns:
                A dictionary with fields (at time of writing) for
                ['name', 'underlying', 'description', 'type', 'expiry', 'perpetual',
               'expired', 'enabled', 'postOnly', 'priceIncrement', 'sizeIncrement',
               'last', 'bid', 'ask', 'index', 'mark', 'imfFactor', 'lowerBound',
               'upperBound', 'underlyingDescription', 'expiryDescription', 'moveStart',
               'marginPrice', 'positionLimitWeight', 'group', 'change1h', 'change24h',
               'changeBod', 'volumeUsd24h', 'volume', 'openInterest',
               'openInterestUsd']"""

        endpoint = "futures/{0}".format(fut_symbol)
        response = self.connector.auth_get_request(endpoint).json()

        if response['success']==False:
            raise Exception(response['error'])

        return response['result']