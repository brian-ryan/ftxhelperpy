import time
import unittest

from ftxhelperpy.mktdata.streamer import OrderBookStream
from unittest.mock import patch

class TestOrderBookStreamMethods(unittest.TestCase):

    def setUp(self) -> None:
        # requires FTX_KEY1 and FTX_SECRET1 env variables to be set
        self.order_streamer = OrderBookStream()
        self.order_streamer.connect()

    def tearDown(self) -> None:
        del self.order_streamer

    def test_subscribe_valid_ticker(self):
        TICKER = 'BTC-PERP'
        self.order_streamer.subscribe_ticker(TICKER)
        time.sleep(2)

        self.assertTrue(self.order_streamer.is_subscribed(TICKER))

    # patching updates so that the test is not contaminated
    # this is specifically testing what happens after a partial method is received
    @patch.object(OrderBookStream, '_handle_update')
    def test_partial_message_ticker(self, mocked_handle_update):
        mocked_handle_update.return_value = None
        TICKER = 'BTC-PERP'
        self.order_streamer.subscribe_ticker(TICKER)
        time.sleep(2)

        checksum_checks_out = self.order_streamer.valid_checksums[TICKER]['is_correct']
        checksum_update_time = self.order_streamer.valid_checksums[TICKER]['updated_at']
        time_since_checksum_update = time.time() - checksum_update_time
        time_since_update = time.time() - self.order_streamer.update_times['BTC-PERP']

        # just check that the checksum was validated within the last 5 seconds
        self.assertLess(time_since_checksum_update, 5)
        self.assertLess(time_since_update, 5)
        # after a partial message is received the checksum should be validated
        self.assertTrue(checksum_checks_out)
        ticker_order_book = self.order_streamer.order_book['BTC-PERP']
        bids = ticker_order_book['bids']
        asks = ticker_order_book['asks']

        self.assertEqual(len(bids), self.order_streamer._ORDER_DEPTH_CHECKSUM)
        self.assertEqual(len(asks), self.order_streamer._ORDER_DEPTH_CHECKSUM)

        # orders should be sorted with most competitive at the front
        for i in range(1, len(bids)):
            self.assertGreaterEqual(bids[i-1], bids[i])

        for i in range(1, len(asks)):
            self.assertLessEqual(asks[i - 1], asks[i])
        print("FINISHED TESTING")

    def test_unsubscribe_ticker(self):
        TICKER = 'BTC-PERP'
        self.order_streamer.subscribe_ticker(TICKER)
        time.sleep(2)
        self.order_streamer.unsubscribe_ticker(TICKER)
        time.sleep(1)

        self.assertFalse(self.order_streamer.is_subscribed(TICKER))
        self.assertNotIn(TICKER, self.order_streamer.order_book)
        self.assertNotIn(TICKER, self.order_streamer.update_times)
        self.assertNotIn(TICKER, self.order_streamer.market_checksums)
        self.assertNotIn(TICKER, self.order_streamer.market_level_pointers)
        self.assertNotIn(TICKER, self.order_streamer.valid_checksums)

    @patch.object(OrderBookStream, '_handle_update')
    def test_update_orders_existing_level(self, patched_handle_update):
        patched_handle_update.return_value = None

        TICKER = 'BTC-PERP'
        self.order_streamer.subscribe_ticker(TICKER)
        time.sleep(2)

        # now BTC-PERP will have received its initial update but
        # we are patching the handle_update method so we can test it manually
        old_order_book = self.order_streamer.order_book[TICKER]
        BID_2 = old_order_book['bids'][2]
        BID_2_PRICE = BID_2[0]
        BID_2_SIZE = BID_2[1]
        NEW_BID_2_SIZE = BID_2_SIZE + 1000

        self.order_streamer._update_orders(
            TICKER,
            'bids',
            [[BID_2_PRICE, NEW_BID_2_SIZE]]
        )

        # check that the size at this level has been updated correctly
        self.assertEqual(NEW_BID_2_SIZE, self.order_streamer.order_book[TICKER]['bids'][2][1])

    @patch.object(OrderBookStream, '_handle_update')
    def test_update_orders_existing_level_removed(self, patched_handle_update):
        patched_handle_update.return_value = None

        TICKER = 'BTC-PERP'
        self.order_streamer.subscribe_ticker(TICKER)
        time.sleep(2)

        old_order_book = self.order_streamer.order_book[TICKER]
        BID_2 = old_order_book['bids'][2]
        BID_2_PRICE = BID_2[0]

        BID_3 = old_order_book['bids'][3]
        BID_3_PRICE = BID_3[0]
        OLD_BID_3_INDEX = self.order_streamer.market_level_pointers[TICKER]['bids'][BID_3_PRICE]

        self.order_streamer._update_orders(
            TICKER,
            'bids',
            [[BID_2_PRICE, 0]]
        )

        #this level should just not exist any more
        self.assertNotIn(BID_2_PRICE, self.order_streamer.market_level_pointers[TICKER]['bids'])

        NEW_BID_3_INDEX = self.order_streamer.market_level_pointers[TICKER]['bids'][BID_3_PRICE]
        # the bid that used to exist in position 3 should now be in position 2
        self.assertEqual(NEW_BID_3_INDEX, OLD_BID_3_INDEX - 1)

    @patch.object(OrderBookStream, '_handle_update')
    def test_update_is_new_level(self, patched_handle_update):
        patched_handle_update.return_value = None

        TICKER = 'BTC-PERP'
        self.order_streamer.subscribe_ticker(TICKER)
        time.sleep(2)

        old_order_book = self.order_streamer.order_book[TICKER]

        OLD_BID_6_PRICE = old_order_book['bids'][6][0]
        # get a price for the new bid
        NEW_BID_EXPECTED_INDEX = 6
        NEW_BID_PRICE = (old_order_book['bids'][5][0] + old_order_book['bids'][6][0]) / 2

        self.order_streamer._update_orders(
            TICKER,
            'bids',
            [[NEW_BID_PRICE, 100]]
        )

        # the new bid should have been inserted at the expected index
        self.assertEqual(NEW_BID_EXPECTED_INDEX, self.order_streamer.market_level_pointers[TICKER]['bids'][NEW_BID_PRICE])
        # the bid which used to be at position 6 should now be at position 7
        self.assertEqual(7, self.order_streamer.market_level_pointers[TICKER]['bids'][OLD_BID_6_PRICE])

if __name__ == '__main__':
    unittest.main()