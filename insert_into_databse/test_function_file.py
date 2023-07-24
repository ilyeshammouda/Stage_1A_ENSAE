from typing import Any

import pandas as pd
import numpy as np
from math import sqrt
import yfinance as yf
from pandas import Series, DataFrame
from pandas.core.generic import NDFrame

import getting_data_from_database_eth
prices=getting_data_from_database_eth.get_ETH_from_database()
prices['returns'] = (np.log(prices.Price /
    prices.Price.shift(-1)))
prices_log=prices['returns'].iloc[1:]
for k in [10,30,60]:
    prices['hist_vol_'+str(k)]=(prices_log.rolling(k).std().dropna())*(sqrt(360*30)/sqrt(29))*100
volume: Series | DataFrame | None | NDFrame | Any=yf.download('ETH-USD', '2017-11-09', '2022-06-03')['Volume']
print('volume')
print(volume)
tests=volume.iloc[0:]
print('tests')
print(tests)
prices['volume']=tests.values
print(prices)