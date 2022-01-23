import os
import unittest

from ftxhelperpy.utils.connect import Connector


class TestConnectMethods(unittest.TestCase):

    @classmethod
    def setUpClass(cls) -> None:
        cls.connector = Connector(os.getenv("FTX_ENDPOINT"))

    def setUp(self) -> None:
        self.connector = TestConnectMethods.connector

    def test_auth_get_request_is_successful(self):
        response = self.connector.auth_get_request("subaccounts")
        is_success = response.json()['success']
        self.assertTrue(is_success)

    def test_auth_post_request_is_successful(self):
        response = self.connector.auth_post_request("subaccounts", {"nickname": "test_sub_account"})
        is_success = response.json()['success']
        self.assertTrue(is_success)

    def test_auth_delete_request_is_successful(self):
        self.connector.auth_post_request("subaccounts", {"nickname": "test_sub_account"})
        response = self.connector.auth_delete_request("subaccounts", {"nickname": "test_sub_account"})
        is_success = response.json()['success']
        self.assertTrue(is_success)

    @classmethod
    def tearDownClass(cls) -> None:
        #ensures that the test sub account that was made is deleted
        cls.connector.auth_delete_request("subaccounts", {"nickname": "test_sub_account"})


if __name__ == '__main__':
    unittest.main()




