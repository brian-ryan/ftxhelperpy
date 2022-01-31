from ftxhelperpy.utils.connect import Connector

class Account:
    def __init__(self, Connector: type[Connector]):
        self.connector = Connector

    def get_info(self) -> dict:
        """Returns a dictionary of information related to the account.

        At time of writing, returns the following fields:
        ['accountIdentifier', 'username', 'collateral', 'freeCollateral',
        'totalAccountValue', 'totalPositionSize', 'initialMarginRequirement',
        'maintenanceMarginRequirement', 'marginFraction', 'openMarginFraction',
        'liquidating', 'backstopProvider', 'positions', 'takerFee', 'makerFee',
        'leverage', 'positionLimit', 'positionLimitUsed', 'useFttCollateral',
        'chargeInterestOnNegativeUsd', 'spotMarginEnabled', 'spotLendingEnabled']"""

        endpoint = "account"
        response = self.connector.auth_get_request(endpoint).json()

        if response['success'] == False:
            raise Exception(response['error'])

        return response['result']