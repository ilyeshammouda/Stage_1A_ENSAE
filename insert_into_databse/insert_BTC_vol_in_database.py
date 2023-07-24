import sqlalchemy
from sqlalchemy.engine import URL
import numpy as np
import get_data_from_database_BTC
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


'''
defining the function that will insert the new table in the data base
'''


def insert_pricers_in_table_alchemy(price_table):
    """ inserts in database """
    connection_string = 'DRIVER='+DRIVER+';SERVER=tcp:'+SERVER+';PORT=1433;DATABASE='+DATABASE+';UID='+USERNAME+';PWD='+ PASSWORD
    connection_url = URL.create("mssql+pyodbc", query={"odbc_connect": connection_string})
    engine = sqlalchemy.create_engine(connection_url)
    price_table.to_sql('BTC_table', con=engine,index=False, if_exists='append')

''''
reading the table from the data_base
'''

BTC_table=get_data_from_database_BTC.get_BTC_from_database()
''' 
calculating the volatility 
'''

BTC_table['returns'] = (np.log(BTC_table.Price /
    BTC_table.Price.shift(-1)))
prices_log=BTC_table['returns'].iloc[1:]

for k in [10,30,60]:
    BTC_table['hist_vol_'+str(k)]=(prices_log.rolling(k).std().dropna())*(sqrt(360*30)/sqrt(29))*100

''''
getting the volume  from yahoo finance, the matching has been done manually 
'''

btc_volume=yf.download('BTC-USD', '2017-11-09', '2022-06-03')['Volume'].iloc[1:]
BTC_table['volume']=btc_volume.values


insert_pricers_in_table_alchemy(BTC_table)