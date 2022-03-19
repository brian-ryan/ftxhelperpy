import os
import pandas as pd
import unittest

from datetime import datetime, timedelta

from ftxhelperpy.mktdata.prices import HistDataFetcher, LiveDataFetcher
from ftxhelperpy.utils.connect import Connector

class TestHistDataFetchMethods(unittest.TestCase):

    @classmethod
    def setUpClass(cls) -> None:
        connector = Connector()
        cls.data_fetcher = HistDataFetcher(connector)

    def setUp(self) -> None:
        self.data_fetcher = TestHistDataFetchMethods.data_fetcher

    def test_get_rates_correct_date_successful(self):
        prices = self.data_fetcher.get_rates("BTC-PERP", datetime.now()-timedelta(hours=1), datetime.now())
        self.assertIsInstance(prices, pd.DataFrame)
        self.assertGreater(len(prices), 0)

    def test_get_index_prices_successful(self):
        prices = self.data_fetcher.get_index_prices("BTC", datetime.now() - timedelta(hours=1), datetime.now())
        self.assertIsInstance(prices, pd.DataFrame)
        self.assertGreater(len(prices), 0)

    def test_get_future_prices_successful(self):
        prices = self.data_fetcher.get_future_prices("BTC-PERP", datetime.now() - timedelta(hours=1), datetime.now())
        self.assertIsInstance(prices, pd.DataFrame)
        self.assertGreater(len(prices), 0)

    def test_get_index_prices_with_resolution_successful(self):
        resolution = 300
        prices = self.data_fetcher.get_index_prices("BTC", datetime.now() - timedelta(hours=1), datetime.now(), resolution)
        self.assertIsInstance(prices, pd.DataFrame)
        self.assertGreater(len(prices), 0)
        # the time difference between rows in milliseconds
        time_jumps = (prices['time'][1] - prices['time'][0]) / 1000
        self.assertEqual(time_jumps, resolution)

    def test_get_index_prices_with_invalid_resolution_throws_exception(self):
        resolution = 350
        with self.assertRaises(Exception) as context:
            self.data_fetcher.get_index_prices("BTC", datetime.now() - timedelta(hours=1), datetime.now(), resolution)
        self.assertTrue('Unsupported candle resolution' in str(context.exception))

    def test_get_index_prices_with_invalid_date_range_is_empty(self):
        prices = self.data_fetcher.get_index_prices("BTC", datetime.now() + timedelta(hours=1), datetime.now())
        self.assertTrue(prices.empty)

    def test_get_rates_invalid_date_range_is_empty_dataframe(self):
        rates = self.data_fetcher.get_rates("SOL-PERP", datetime.now() + timedelta(hours=1), datetime.now() + timedelta(hours=2))
        self.assertTrue(rates.empty)

    def test_get_future_prices_invalid_date_range_is_empty_dataframe(self):
        prices = self.data_fetcher.get_future_prices("BTC-PERP", datetime.now() + timedelta(hours=1), datetime.now() + timedelta(hours=2))
        self.assertTrue(prices.empty)

    def test_get_future_prices_with_resolution_successful(self):
        resolution = 300
        prices = self.data_fetcher.get_future_prices("BTC-PERP", datetime.now() - timedelta(hours=1), datetime.now(), resolution)

        self.assertIsInstance(prices, pd.DataFrame)
        self.assertGreater(len(prices), 0)

        #the time difference between rows in milliseconds
        time_jumps = (prices['time'][1] - prices['time'][0])/1000
        self.assertEqual(time_jumps, resolution)

    def test_historical_funding_rates_valid_args_successful(self):
        rates = self.data_fetcher.get_rates("SOL-PERP", datetime.now() - timedelta(hours=10), datetime.now())
        self.assertIsInstance(rates, pd.DataFrame)
        self.assertGreater(len(rates), 0)

    def test_get_trades_valid_args_is_successful(self):
        trades = self.data_fetcher.get_trades("BTC-PERP", datetime.now() - timedelta(hours=1), datetime.now())
        self.assertIsInstance(trades, pd.DataFrame)
        self.assertGreater(len(trades), 0)

    def test_get_trades_invalid_future_raises_exception(self):
        with self.assertRaises(Exception) as context:
            self.data_fetcher.get_trades("AA312", datetime.now() - timedelta(hours=1), datetime.now())
        self.assertTrue('No such market' in str(context.exception))

    def test_get_trades_invalid_dates_is_empty_dataframe(self):
        trades = self.data_fetcher.get_trades("BTC-PERP", datetime.now() + timedelta(hours=1), datetime.now()+timedelta(hours=2))
        self.assertTrue(trades.empty)

    def test_get_future_prices_invalid_resolution_raises_exception(self):
        resolution = 350
        with self.assertRaises(Exception) as context:
            self.data_fetcher.get_future_prices("BTC-PERP", datetime.now() - timedelta(hours=1),  datetime.now(), resolution)
        self.assertTrue('Unsupported candle resolution' in str(context.exception))

    def test_get_prices_invalid_symbol_raises_exception(self):
        with self.assertRaises(Exception) as context:
            self.data_fetcher.get_future_prices("AA321", datetime.now() + timedelta(hours=1), datetime.now() + timedelta(hours=2))
        self.assertTrue('No such market' in str(context.exception))

    def test_get_rates_invalid_symbol_raises_exception(self):
        with self.assertRaises(Exception) as context:
            self.data_fetcher.get_rates("ACBX99", datetime.now() - timedelta(hours=10), datetime.now())
        self.assertTrue('No such future' in str(context.exception))

class TestLiveDataFetchMethods(unittest.TestCase):

    @classmethod
    def setUpClass(cls) -> None:
        connector = Connector()
        cls.data_fetcher = LiveDataFetcher(connector)

    def setUp(self) -> None:
        self.data_fetcher = TestLiveDataFetchMethods.data_fetcher

    def test_get_order_book_valid_symbol_successful(self):
        order_book = self.data_fetcher.get_order_book("BTC-PERP")

        self.assertTrue('bids' in order_book)
        self.assertTrue('asks' in order_book)

        self.assertTrue(type(order_book['bids'])==list)
        self.assertTrue(type(order_book['asks']) == list)

        self.assertTrue('price' in order_book['bids'][0])
        self.assertTrue('size' in order_book['bids'][0])

    def test_get_order_book_with_depth_successful(self):
        depth = 3
        order_book = self.data_fetcher.get_order_book("BTC-PERP", depth)
        self.assertEqual(len(order_book['bids']), depth)

    def test_get_orderbook_invalid_symbol_raises_exception(self):
        with self.assertRaises(Exception) as context:
            self.data_fetcher.get_order_book("AA312")
        self.assertTrue('No such market' in str(context.exception))

if __name__ == '__main__':
    unittest.main()