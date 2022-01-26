import os
import pandas as pd
import unittest

from ftxhelperpy.mktdata.meta import MetaDataFetcher
from ftxhelperpy.utils.connect import Connector

class TestMetaDataFetchMethods(unittest.TestCase):

    @classmethod
    def setUpClass(cls) -> None:
        connector = Connector(os.getenv("FTX_ENDPOINT"))
        cls.data_fetcher = MetaDataFetcher(connector)

    def setUp(self) -> None:
        self.data_fetcher = TestMetaDataFetchMethods.data_fetcher

    def test_get_all_futures_is_successful(self):
        futures = self.data_fetcher.get_all_futures()
        self.assertIsInstance(futures, pd.DataFrame)
        self.assertGreater(len(futures), 0)

    def test_get_all_futures_sort_by_is_successful(self):
        futures = self.data_fetcher.get_all_futures(sort_by='change1h')
        col_list = list(futures['change1h'])
        self.assertTrue(all(col_list[i] >= col_list[i+1] for i in range(len(col_list)-1)))

    def test_get_future_is_successful(self):
        future_dict = self.data_fetcher.get_future('BTC-PERP')
        self.assertIsInstance(future_dict, dict)
        self.assertFalse(len(future_dict)==0)

    def test_get_future_invalid_symbol_raises_exception(self):
        with self.assertRaises(Exception) as context:
            self.data_fetcher.get_future('SHIT-COIN--PERP')
        self.assertTrue('No such future' in str(context.exception))

if __name__ == '__main__':
    unittest.main()




