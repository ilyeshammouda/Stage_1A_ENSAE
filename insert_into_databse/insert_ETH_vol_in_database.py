import sqlalchemy
from sqlalchemy.engine import URL
import numpy as np
import getting_data_from_database_eth
import pandas as pd
from math import sqrt
import yfinance as yf

''''
specifying the information that will be needed to connect to the data base
'''


SERVER = 'Put here your Server adress'
DATABASE = 'Put here your database name'
USERNAME = 'Put here your username'
PASSWORD = 'Put here the passeword to connect to the Azure database'
DRIVER= '{ODBC Driver 17 for SQL Server}'


''''
defining the function that will insert the new table in the data base
'''


def insert_pricers_in_table_alchemy(price_table):
    """ inserts in database """
    connection_string = 'DRIVER='+DRIVER+';SERVER=tcp:'+SERVER+';PORT=1433;DATABASE='+DATABASE+';UID='+USERNAME+';PWD='+ PASSWORD
    connection_url = URL.create("mssql+pyodbc", query={"odbc_connect": connection_string})
    engine = sqlalchemy.create_engine(connection_url)
    price_table.to_sql('ETH_table', con=engine,index=False, if_exists='append')

''''
reading the table from the data_base
'''
ETH_table=getting_data_from_database_eth.get_ETH_from_database()

''' 
calculating the volatility 
'''


ETH_table['returns'] = (np.log(ETH_table.Price /
    ETH_table.Price.shift(-1)))
prices_log=ETH_table['returns'].iloc[1:]
for k in [10,30,60]:
    ETH_table['hist_vol_'+str(k)]=(prices_log.rolling(k).std().dropna())*(sqrt(360*30)/sqrt(29))*100

''''
getting the volume  from yahoo finance, the matching has been done manually 
'''

ETH_volume=yf.download('ETH-USD', '2017-11-09', '2022-06-03')['Volume'].iloc[0:]

ETH_table['volume']=ETH_volume.values

insert_pricers_in_table_alchemy(ETH_table)