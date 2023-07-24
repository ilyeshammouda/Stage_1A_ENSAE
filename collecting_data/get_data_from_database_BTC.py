import sqlalchemy
from sqlalchemy.engine import URL
import pandas as pd
SERVER = 'como-trading.database.windows.net'
DATABASE = 'como-risk'
USERNAME = 'como-admin'
PASSWORD = 'CRDpls-2041!'
DRIVER= '{ODBC Driver 17 for SQL Server}'
def get_BTC_from_database():
    """ creates table with alchemy"""
    connection_string = 'DRIVER='+DRIVER+';SERVER=tcp:'+SERVER+';PORT=1433;DATABASE='+DATABASE+';UID='+USERNAME+';PWD='+ PASSWORD
    connection_url = URL.create("mssql+pyodbc", query={"odbc_connect": connection_string})
    engine = sqlalchemy.create_engine(connection_url)
    table_df = pd.read_sql('''SELECT * FROM Crypto_Mkt WHERE Instrument_id = 'BTC/USD'  ''', con=engine)
    return table_df
