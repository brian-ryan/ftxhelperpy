import os
import pandas as pd
import unittest

from datetime import datetime, timedelta

from ftxhelperpy.mktdata.historic import HistDataFetcher
from ftxhelperpy.utils.connect import Connector

class TestHistDataFetchMethods(unittest.TestCase):

    @classmethod
    def setUpClass(cls) -> None:
        connector = Connector(os.getenv("FTX_ENDPOINT"))
        cls.data_fetcher = HistDataFetcher(connector)

    def setUp(self) -> None:
        self.data_fetcher = TestHistDataFetchMethods.data_fetcher

    def test_get_historical_prices_correct_date_successful(self):
        prices = self.data_fetcher.get_historical_prices("BTC-PERP", datetime.now()-timedelta(hours=1), datetime.now())
        self.assertIsInstance(prices, pd.DataFrame)
        self.assertGreater(len(prices), 0)

    def test_get_historical_prices_invalid_date_range_is_empty_dataframe(self):
        rates = self.data_fetcher.get_historical_rates("SOL-PERP", datetime.now() + timedelta(hours=1), datetime.now() + timedelta(hours=2))
        self.assertTrue(rates.empty)

    def test_get_historical_rates_invalid_date_range_is_empty_dataframe(self):
        prices = self.data_fetcher.get_historical_prices("BTC-PERP", datetime.now() + timedelta(hours=1), datetime.now() + timedelta(hours=2))
        self.assertTrue(prices.empty)

    def test_get_historical_prices_with_resolution_successful(self):
        resolution = 300
        prices = self.data_fetcher.get_historical_prices("BTC-PERP", datetime.now() - timedelta(hours=1), datetime.now(), resolution)

        self.assertIsInstance(prices, pd.DataFrame)
        self.assertGreater(len(prices), 0)

        #the time difference between rows in milliseconds
        time_jumps = (prices['time'][1] - prices['time'][0])/1000
        self.assertEqual(time_jumps, resolution)

    def test_historical_funding_rates_valid_args_successful(self):
        rates = self.data_fetcher.get_historical_rates("SOL-PERP", datetime.now() - timedelta(hours=10), datetime.now())
        self.assertIsInstance(rates, pd.DataFrame)
        self.assertGreater(len(rates), 0)

    def test_get_historical_prices_invalid_resolution_raises_exception(self):
        resolution = 350
        with self.assertRaises(Exception) as context:
            self.data_fetcher.get_historical_prices("BTC-PERP", datetime.now() - timedelta(hours=1),  datetime.now(), resolution)

        self.assertTrue('Unsupported candle resolution' in str(context.exception))

    def test_get_historical_prices_invalid_symbol_raises_exception(self):
        with self.assertRaises(Exception) as context:
            self.data_fetcher.get_historical_prices("AA321", datetime.now() + timedelta(hours=1), datetime.now() + timedelta(hours=2))

        self.assertTrue('No such market' in str(context.exception))

    def test_get_historical_rates_invalid_symbol_raises_exception(self):
        with self.assertRaises(Exception) as context:
            self.data_fetcher.get_historical_rates("ACBX99", datetime.now() - timedelta(hours=10), datetime.now())

        self.assertTrue('No such future' in str(context.exception))

if __name__ == '__main__':
    unittest.main()




