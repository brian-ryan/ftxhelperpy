import hmac
import os
import json
import time
import zlib

from bisect import bisect
from threading import Thread
from websocket import WebSocketApp

class Streamer:

    _SOCKET_URL = "wss://ftx.com/ws/"
    _CONNECTION_TIMEOUT_SECS = 5

    def __init__(self):
        # the web socket app
        self.ws = None
        # the thread within which the web socket will run
        self.ws_thread = None
        # whether or not the initial connection was successful
        self.init_connect_success = False
        # whether or not we are logged in
        self.logged_in = False

    def _on_message(self, ws: WebSocketApp, msg) -> None:
        """Child classes must implement this method to handle
        incoming messages from the web socket

        Parameters:
            ws: Instance of WebSocketApp
            msg: The message passed as part of
            the callback
        """

        raise NotImplementedError

    def _on_error(self, ws: WebSocketApp, err) -> None:
        """If an error occurs in the web socket this will
        raise an exception and print out the error message

        Parameters:
            ws: Instance of WebSocketApp
            err: The error message passed as part of
            the callback
        """

        print("Error occurred in web socket")
        raise Exception(err)

    def _init_socket(self) -> None:
        """Creates instance of the web socket app"""
        self.ws = WebSocketApp(self._SOCKET_URL, on_message = self._on_message, on_error = self._on_error)

    def _check_connection(self) -> None:
        """Checks the connection in websocket
        at initialisation. Will raise exception if connection
        is not established within the _INIT_TIMEOUT_SECS"""

        time_start = time.time()
        while (not self.ws.sock or not self.ws.sock.connected):
            if time.time() - time_start > self._CONNECTION_TIMEOUT_SECS:
                raise Exception("Initial connection to websocket failed in {0}".format(self.__class__.__name__))
            time.sleep(0.1)

        print("Initial websocket connection successful in {0}".format(self.__class__.__name__))
        self.init_connect_success = True

    def _run_socket(self, ws):
        ws.run_forever()

    def _init_connect(self):
        """Establishes initial connection to FTX server

        If the connection is not successful this method will
        raise exception from the _check_connection method.
        """

        self._init_socket()
        self.ws_thread = Thread(target = self._run_socket, args = (self.ws, ))
        self.ws_thread.daemon = True
        self.ws_thread.start()
        self._check_connection()

    def _subscribe(self, sub_params: dict):
        """Sends subscription message to server

        Parameters:
            sub_params: Dictionary of parameters
            representing the subscription
        """
        self.send({'op': 'subscribe', **sub_params})

    def _unsubscribe(self, unsub_params: dict):
        """Sends unsubscribe message to server

            Parameters:
                unsub_params: Dictionary of parameters
                representing the unsubscription
        """

        self.send({'op': 'unsubscribe', **unsub_params})

    def _login(self) -> None:
        """Authenticates ourself to the server.

        Requires FTX_KEY1 and FTX_SECRET1 env
        variables to be set
        """

        ts = int(time.time() * 1000)
        self.send({
            'op': 'login',
            'args': {
                'key': os.getenv('FTX_KEY1'),
                'sign': hmac.new(os.getenv('FTX_SECRET1').encode(), f'{ts}websocket_login'.encode(), 'sha256').hexdigest(),
                'time': ts
            }
        })
        self.logged_in = True

    def send(self, msg: dict) -> None:
        """Sends a message to the server

        Parameters:
           msg: dictionary for the message we want
           to send to server
        """

        if self.init_connect_success == False:
            print("Error: Connection not established")
            return

        self.ws.send(json.dumps(msg))

    def connect(self):
        if self.ws == None:
            print("Initialising connection to {0}".format(self._SOCKET_URL))
            self._init_connect()
            self._login()

class OrderBookStream(Streamer):

    _ORDER_DEPTH_CHECKSUM = 100
    def __init__(self):
        super().__init__()
        # dictionary where each key is a market and value is the order book
        self.order_book = {}
        self._subscribed_list = []
        # for each market subscribed, the time of the last update received
        self.update_times = {}
        # checksums to ensure our current state is correct
        self.market_checksums = {}
        # keeping a pointer to position of each level
        self.market_level_pointers = {}
        # dictionary storing whether the last checksum check was valid
        self.valid_checksums = {}
        # stores tickers which we want to resubscribe to
        self.resubscribe_requests = {}

    def _handle_partial(self, msg: dict):
        """Handles a partial (i.e. full state) message

        Parameters:
            msg: Dictionary representing the message
        """

        ticker = msg['market']
        self.update_times[ticker] = msg['data']['time']
        self.market_checksums[ticker] = msg['data']['checksum']
        self.order_book[ticker] = {
            'bids': msg['data']['bids'],
            'asks': msg['data']['asks']
        }

        # this is an expensive op but it is only carried out once
        # per market when the initial state is received
        self.market_level_pointers[ticker] = {'bids': {}, 'asks': {}}
        bids = msg['data']['bids']
        asks = msg['data']['asks']

        for i in range(len(bids)):
            price = bids[i][0]
            self.market_level_pointers[ticker]['bids'][price] = i

        for i in range(len(asks)):
            price = asks[i][0]
            self.market_level_pointers[ticker]['asks'][price] = i

        self.validate_checksum(ticker)

    def generate_raw_checksum_string(self, bids: list, asks: list):
        """Generates the raw string to perform crc32 on
        from the array of bids and asks. See
        https://docs.ftx.com/#orderbooks for more details
        """

        raw_str = ""

        for i in range(self._ORDER_DEPTH_CHECKSUM):

            if i < len(bids):
                raw_str += "{0}:{1}:".format(bids[i][0], bids[i][1])

            if i < len(asks):
                raw_str += "{0}:{1}:".format(asks[i][0], asks[i][1])

        # remove trailing colon if it exists
        if raw_str[-1] == ":":
            raw_str = raw_str[:-1]

        return raw_str

    def validate_checksum(self, ticker: str) -> bool:
        """Checks if the checksum checks out (i.e. that
        our current representation of the order book for this
        ticker is valid). Updates the valid_checksums dictionary
        with the result, and returns a boolean indicating if
        check was correct

        Parameters:
            ticker: String representing the ticker we are
            validating market for
        Returns:
            boolean which is true if the checksum was correct
        """

        raw_string = self.generate_raw_checksum_string(self.order_book[ticker]['bids'],
                                                  self.order_book[ticker]['asks'])

        our_checksum = zlib.crc32(str.encode(raw_string))

        is_correct = (our_checksum == self.market_checksums[ticker])
        self.valid_checksums[ticker] = {'is_correct': is_correct, 'updated_at': time.time()}
        return is_correct

    def _handle_subscribe(self, msg: dict):
        """Handler for subscribed messages"""

        print("Successfully subscribed to {0}".format(msg['market']))
        self._subscribed_list.append(msg['market'])

    def _handle_unsubscribe(self, msg: dict):
        """Handler for unsubscribed messages"""

        ticker = msg['market']

        print("Successfully unsubscribed to {0}".format(ticker))
        self._subscribed_list = list(filter(lambda x: x!=ticker, self._subscribed_list))

        # delete all of the data that is tracked for the ticker
        self.update_times.pop(ticker, None)
        self.market_checksums.pop(ticker, None)
        self.order_book.pop(ticker, None)
        self.market_level_pointers.pop(ticker, None)
        self.valid_checksums.pop(ticker, None)

        # if we wanted to resubscribe then do that here
        if ticker in self.resubscribe_requests:
            self.subscribe_ticker(ticker)
            del self.resubscribe_requests[ticker]

    def _update_orders(self, ticker: str, side: str, updates: list[str]):
        """Handles updating our record of the orderbook

        Parameters:
            ticker: a string representing the ticker that was updated
            side: A string which must be either bids/asks
            updates: A list of lists, where each list in the list
            is of the form [price, size], representing an update to the
            size at that prices
        """

        for update in updates:
            price = update[0]
            size = update[1]

            if price in self.market_level_pointers[ticker][side]:
                self._handle_update_to_existing_level(ticker, side, price, size)
            else:
                self._handle_new_level(ticker, side, price, size)

    def get_level(self, ticker: str, side: str, level: int) -> list:
        """Returns the details [price, size] of the order at
        this level for this ticker on this side

        Parameters:
            ticker: String representing the ticker
            side: Either 'bids' or 'asks'
            level: Integer for the level to return
        """

        return self.order_book[ticker][side][level]

    def _get_insert_index(self, side: str, existing_prices: list[float], price: float) -> int:
        """Returns the index at which we should insert this price

        Parameters:
            side: String whichis either bids/asks
            existing_prices: list of current prices
            price: New price for insert
        Returns:
            int for the index we should insert this price at
        """

        if side == 'asks':
            # if it is ask then it is sorted
            return bisect(existing_prices, price)
        else:
            # need to bisect on the reversed list since bids are sorted descending
            return len(existing_prices) - bisect(list(reversed(existing_prices)), price)

    def _handle_new_level(self, ticker: str, side: str, price: float, size: float):
        """Handles update which includes a new level on this side

        Parameters:
            ticker: String representing the ticker which is updated
            side: String which is either bids/asks
            price: The price of the new level
            size: The size at the new level
        """

        existing_levels = self.order_book[ticker][side]
        # get array of prices for this side
        existing_prices = list(map(lambda x: x[0], existing_levels))
        # find the correct insert index for this price
        insert_index = self._get_insert_index(side, existing_prices, price)
        # insert a new level at this index on this side of the book
        self.order_book[ticker][side].insert(insert_index, [price, size])
        # now bump the pointer for all levels after this level
        self.market_level_pointers[ticker][side][price] = insert_index

        for i in range(insert_index + 1, len(self.order_book[ticker][side])):
            price_at_level = self.order_book[ticker][side][i][0]
            # these have all been moved back one
            self.market_level_pointers[ticker][side][price_at_level] += 1

    def _handle_update_to_existing_level(self, ticker: str, side: str, price: float, size: float):
        """Handles an update in the order book to a level
        which already existed but just had a size change

        Parameters:
            ticker: A string representing the ticker which had an order book update
            side: A string which is either bids/asks
            price: The price which had an update
            size: The size now at the given price
        """

        if size <= 0:
            # order at this level is now non-existent
            # find where this level is in the book
            previous_index = self.market_level_pointers[ticker][side][price]
            # delete the level
            del self.order_book[ticker][side][previous_index]
            # pointers now need to be updated for everything that was after this level
            # as they have now been shifted down 1
            for i in range(previous_index, len(self.order_book[ticker][side])):
                price_at_level = self.order_book[ticker][side][i][0]
                self.market_level_pointers[ticker][side][price_at_level] -= 1

            del self.market_level_pointers[ticker][side][price]
        else:
            # if it is an existing level on this side then the size just needs to be updated
            # update the level at the index corresponding to this price to have the new size
            self.order_book[ticker][side][self.market_level_pointers[ticker][side][price]][1] = size

    def _handle_update(self, msg: dict):
        """Handler for update messages"""

        ticker = msg['market']

        self._update_orders(ticker, 'bids', msg['data'].get('bids', []))
        self._update_orders(ticker, 'asks', msg['data'].get('asks', []))

        self.update_times[ticker] = msg['data']['time']
        self.market_checksums[ticker] = msg['data']['checksum']

    def _on_message(self, ws, msg):
        msg = json.loads(msg)
        if msg['type'] == 'partial':
            self._handle_partial(msg)
        elif msg['type'] == 'update':
            self._handle_update(msg)
        elif msg['type'] == 'subscribed':
            self._handle_subscribe(msg)
        elif msg['type'] == 'unsubscribed':
            self._handle_unsubscribe(msg)

    def unsubscribe_ticker(self, ticker: str):
        """Unsubscribes to the ticker

        Parameters:
            ticker: String of the ticker to which
            we want to unsubscribe
        """

        self._unsubscribe({'channel': 'orderbook', 'market': ticker})

    def resubscribe_ticker(self, ticker: str):
        """Unsubscribes and then resubscribes to a ticker"""

        print("Resubscribing to {}".format(ticker))
        self.resubscribe_requests[ticker] = ticker
        self.unsubscribe_ticker(ticker)

    def subscribe_ticker(self, ticker: str):
        if ticker in self._subscribed_list:
            print("Already subscribed to {0}",format(ticker))
            return

        print("Subscribing to orderbook for {0}".format(ticker))
        self._subscribe({'channel': 'orderbook', 'market': ticker})

    def is_subscribed(self, ticker: str) -> bool:
        """Checks if we have subscribed to the given ticker

        Parameters:
            ticker: string of ticker we want to check
            subscription to

        Returns:
            bool: Boolean which is true if we are
            subscribed to the ticker
        """

        subscribed =  (ticker in self._subscribed_list)
        return subscribed