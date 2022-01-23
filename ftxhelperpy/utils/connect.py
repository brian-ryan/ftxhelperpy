import hmac
import os
from requests import Request, Session, PreparedRequest, Response
import time

class Connector:

    def __init__(self, api_endpoint):
        self.create_session()
        self.api_endpoint = api_endpoint

    def create_session(self) -> None:
        self.session = Session()

    def add_auth_headers(self, prepared_request: type[PreparedRequest]) -> PreparedRequest:
        """Adds authentication headers to the request.

        Args:
            request: The Request object

        Returns:
            A PreparedRequest object
        """

        ts = int(time.time() * 1000)
        signature = self.get_signature(prepared_request, ts)
        prepared_request.headers['FTX-KEY'] = os.getenv('FTX_KEY1')
        prepared_request.headers['FTX-SIGN'] = signature
        prepared_request.headers['FTX-TS'] = str(ts)
        return prepared_request

    def get_signature(self, prepared_request: type[PreparedRequest], ts: int) -> hmac:
        """Returns a hmac signature for a request

        Args:
            prepared_request: The PreparedRequest object
            ts: The time of the request. Must match FTX-TS in request header

        Returns:
            A hmac object"""

        signature_payload = f'{ts}{prepared_request.method}{prepared_request.path_url}'.encode()
        if prepared_request.body:
            signature_payload = signature_payload + prepared_request.body

        signature = hmac.new(os.getenv('FTX_SECRET1').encode(), signature_payload, 'sha256').hexdigest()
        return signature

    def auth_get_request(self, endpoint: str, query_params: dict = {}) -> Response:
        """Makes an authenticated GET request.

        Args:
            endpoint: The endpoint of the request

        Returns:
            A Response object from the GET request
        """

        full_path = self.api_endpoint + "/" + endpoint
        prepared_request = Request('GET', full_path).prepare()
        prepared_request.prepare_url(prepared_request.url, query_params)
        prepared_request = self.add_auth_headers(prepared_request)
        response = self.session.send(prepared_request)
        return response

    def auth_post_request(self, endpoint: str, payload: dict = {}) -> Response:
        """Makes an authenticated POST request.

           Args:
              endpoint: The endpoint of the request
              payload: A dictionary of payload to include in request

           Returns:
              A Response object from the GET request
        """

        full_path = self.api_endpoint + "/" + endpoint
        prepared_request = Request('POST', full_path, json = payload).prepare()
        prepared_request = self.add_auth_headers(prepared_request)
        response = self.session.send(prepared_request)
        return response

    def auth_delete_request(self, endpoint: str, payload: dict={}) -> Response:
        """Makes an authenticated POST request.

           Args:
              endpoint: The endpoint of the request
              payload: A dictionary of payload to include in request

           Returns:
              A Response object from the GET request
        """

        full_path = self.api_endpoint + "/" + endpoint
        prepared_request = Request('DELETE', full_path, json=payload).prepare()
        prepared_request = self.add_auth_headers(prepared_request)
        response = self.session.send(prepared_request)
        return response
