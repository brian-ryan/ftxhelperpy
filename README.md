# ftxhelperpy
A repo focused on creating useful resources for interacting with the FTX api. The premise of this work is that to interact with FTX successfully with the ultimate goal of generating PnL, there are a number of areas which we can focus on 

**Generic Utility Funcions**

These live in the utils package. Generic, useful and common functions which will be used everywhere. For example, sending an authenticated GET request. Pretty much anything which I couldn't logically add to any of the other packages which have generic utility. 

**Historic Data**

This lives in the historic package. It is mostly made up of functions which make the retrieval of historical data for different instruments on FTX a little easier to manage.

**Account Management**

This lives in the accounts package. Part of any trading strategy is managing your accounts, your collateral, your margin levels etc. The accounts package provides useful functions for retrieving and updating information related to your account. 

**Trading**

This lives in the trading package. Anything related to order placement, order retrieval, cancellation, modification etc is placed here. 
