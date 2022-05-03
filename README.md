# ftxhelperpy

**Setup**

1. Create a python3 virtual environment


```
python3 -m venv ftxenv
```

2. Activate the virtual env

```
source ftxenv/bin/activate
```

3. Install the package

```
pip install git+https://github.com/BrianRyan94/ftxhelperpy.git
```

**Creating a Connector**

The connector is a dependency of pretty much all other classes. It just manages things like constructing URLs, authenticating requests etc. 

1. Set the FTX_ENDPOINT env variable. Likely won't change unless they version their API or something

```
export FTX_ENDPOINT=https://ftx.com/api
```

2. Set the FTX_KEY1 variable. You can create an API key on your profile page https://ftx.com/profile 

```
export FTX_KEY1=your_api_key
```

3. Set the FTX_SECRET1 variable. You can create your API secret at the profile page https://ftx.com/profile 

```
export FTX_SECRET1=your_secret_key
```

I'd recommend just adding these 3 env variables to your bashrc or something. 

4. Create an instance of the Connector. If this works, its probably all set up correctly

```
from ftxhelperpy.utils.connector import Connector
connector = Connector()
``` 




