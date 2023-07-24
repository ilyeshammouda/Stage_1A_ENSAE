
import pandas as pd
from IPython.display import display
import sqlalchemy
from sqlalchemy.engine import URL
import numpy as np
from math import sqrt
import collecting_data


SERVER = 'Put here your Server adress'
DATABASE = 'Put here your database name'
USERNAME = 'Put here your username'
PASSWORD = 'Put here the passeword to connect to the Azure database'
DRIVER= '{ODBC Driver 17 for SQL Server}'

'''
Define the function that will update the vol in the table VolCurve_Overide of the como-front database
'''

def update_vol_in_table_alchemy(price_table):
    """ inserts in database """
    connection_string = 'DRIVER='+DRIVER+';SERVER=tcp:'+SERVER+';PORT=1433;DATABASE='+DATABASE+';UID='+USERNAME+';PWD='+ PASSWORD
    connection_url = URL.create("mssql+pyodbc", query={"odbc_connect": connection_string})
    engine = sqlalchemy.create_engine(connection_url)
    price_table.to_sql('VolCurve_Overide', con=engine,index=False, if_exists='append')
'''
read   data
'''
df = collecting_data.select_option('2 days', 'BTC')
df['instrumen']="BTC/USD"
df2=collecting_data.select_option('2 days', 'ETH')
df2['instrumen']="ETH/USD"
'''
organize the dataframe
'''
BTC_data= pd.DataFrame()
BTC_data['Underlying']=df['instrumen']
BTC_data['Strike']=df['strike']
BTC_data['Expiry']=df['expiry']
BTC_data['ImpliedVol']=df['mid_volatility']

ETH_data= pd.DataFrame()
ETH_data['Underlying']=df2['instrumen']
ETH_data['Strike']=df2['strike']
ETH_data['Expiry']=df2['expiry']
ETH_data['ImpliedVol']=df2['mid_volatility']
'''
drop duplicates 
'''


BTC_without_duplicates_table=BTC_data.drop_duplicates()
ETH_without_duplicates_table=ETH_data.drop_duplicates()


display(BTC_without_duplicates_table)
display(ETH_without_duplicates_table)

#update_vol_in_table_alchemy(BTC_data)
