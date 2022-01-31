import os
import unittest

from ftxhelperpy.accounts.core import Account
from ftxhelperpy.utils.connect import Connector

class TestAccountMethods(unittest.TestCase):

    @classmethod
    def setUpClass(cls) -> None:
        connector = Connector(os.getenv("FTX_ENDPOINT"))
        cls.account = Account(connector)

    def setUp(self) -> None:
        self.account = TestAccountMethods.account

    def test_get_info(self):
        info = self.account.get_info()
        self.assertIsInstance(info, dict)
        self.assertGreater(len(info), 0)

if __name__ == '__main__':
    unittest.main()