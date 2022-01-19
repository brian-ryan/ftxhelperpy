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

    def add_auth_headers(self, request) -> PreparedRequest:
        """Adds authentication headers to the request.

        Args:
            request: The Request object

        Returns:
            A PreparedRequest object
        """

        ts = int(time.time() * 1000)
        prepared = request.prepare()
        signature = self.get_signature(prepared, ts)
        prepared.headers['FTX-KEY'] = os.getenv('FTX_KEY1')
        prepared.headers['FTX-SIGN'] = signature
        prepared.headers['FTX-TS'] = str(ts)
        return prepared

    def get_signature(self, prepared_request, ts) -> hmac:
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

    def auth_get_request(self, endpoint) -> Response:
        """Makes an authenticated GET request.

        Args:
            endpoint: The endpoint of the request

        Returns:
            A Response object from the GET request
        """

        full_path = self.api_endpoint + "/" + endpoint
        request = Request('GET', full_path)
        prepared_request = self.add_auth_headers(request)
        response = self.session.send(prepared_request)
        return response

    def auth_post_request(self, endpoint, payload = {}) -> Response:
        """Makes an authenticated POST request.

           Args:
              endpoint: The endpoint of the request
              payload: A dictionary of payload to include in request

           Returns:
              A Response object from the GET request
        """

        full_path = self.api_endpoint + "/" + endpoint
        request = Request('POST', full_path, json = payload)
        prepared_request = self.add_auth_headers(request)
        response = self.session.send(prepared_request)
        return response

    def auth_delete_request(self, endpoint, payload={}) -> Response:
        """Makes an authenticated POST request.

           Args:
              endpoint: The endpoint of the request
              payload: A dictionary of payload to include in request

           Returns:
              A Response object from the GET request
        """

        full_path = self.api_endpoint + "/" + endpoint
        request = Request('DELETE', full_path, json=payload)
        prepared_request = self.add_auth_headers(request)
        response = self.session.send(prepared_request)
        return response
