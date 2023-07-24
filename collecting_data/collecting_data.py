import datetime
import requests
import pandas as pd
import numpy as np
import seaborn as sns
from sklearn.preprocessing import StandardScaler
from matplotlib.pyplot import figure
from IPython.display import display




""" interface Deribit requests """
DERIBIT_ADDRESS = "https://test.deribit.com/api/v2/public/"


class OrderBook():
    """ An order in the market (bid, ask, mid, etc.)"""

    def __init__(
            self,
            instrument,
            mid_volatility,
            date,
            expiry,
            strike,
            call_put,
            spot):
        """ constructor"""
        self.instrument = instrument
        self.mid_volatility = mid_volatility
        self.date = date
        self.expiry = expiry
        self.strike = strike
        self.call_put = call_put
        self.spot = spot

    def to_dict(self):
        """ dictionnary"""
        return {
            'instrument': self.instrument,
            'mid_volatility': self.mid_volatility,
            'date': self.date,
            'expiry': self.expiry,
            'strike': self.strike,
            'call_put': self.call_put,
            'spot': self.spot
        }


def create_get_instruments_message(currency):
    """ create message """
    method = "get_instruments?"
    currency_field = "currency=" + currency
    expired_field = "expired=false"
    kind_field = "kind=option"
    msg = DERIBIT_ADDRESS + method + currency_field + "&" + expired_field + "&" + kind_field
    return msg


def get_options(currency):
    """ get all options definitions """
    request = create_get_instruments_message(currency)
    answer = requests.get(request).json()
    if 'error' in answer:
        print("Deribit error " + answer['error']['message'])
        return

    instruments = answer['result']
    instruments = pd.DataFrame(instruments)
    instruments['expiration_date'] = pd.to_datetime(instruments['expiration_timestamp'].astype(int), unit='ms')
    instruments.drop(['expiration_timestamp'], axis=1, inplace=True)
    instruments['creation_date'] = pd.to_datetime(instruments['creation_timestamp'].astype(int), unit='ms')
    instruments.drop(['creation_timestamp'], axis=1, inplace=True)
    return instruments


def get_expiry_from_ticker(option_ticker):
    """ Computes expiry from ticker"""
    # as day can be of only 1 digit, need to check the date lenght
    lenght = option_ticker.split('-')[1]
    if len(lenght) == 7:
        month = month_to_number(option_ticker[6:9])
        day = int(option_ticker[4:6])
        year = int(option_ticker[9:11]) + 2000
    elif len(lenght) == 6:
        month = month_to_number(option_ticker[5:8])
        day = int(option_ticker[4:5])
        year = int(option_ticker[8:10]) + 2000
    else:
        raise ValueError(f"ticker no valid date: {option_ticker}")
    return datetime.datetime(year, month, day)


def month_to_number(short_month):
    """ month MMM to number"""
    return {
        'JAN': 1,
        'FEB': 2,
        'MAR': 3,
        'APR': 4,
        'MAY': 5,
        'JUN': 6,
        'JUL': 7,
        'AUG': 8,
        'SEP': 9,
        'OCT': 10,
        'NOV': 11,
        'DEC': 12
    }[short_month]


def get_call_put_from_ticker(ticker):
    """ converts ticker to call put"""
    letter = ticker.split('-')[3]
    if letter.casefold() == 'p'.casefold():
        return -1
    elif (letter.casefold() == 'c'.casefold()):
        return 1
    else:
        return float('nan')


def create_order_book_message(instrument):
    """ create book order deribit message to get last 24 hours trades if any """
    method = "get_order_book?"
    instrument_field = "instrument_name=" + instrument

    message = DERIBIT_ADDRESS + method + instrument_field
    return message


def get_order_book(instrument):
    """ get order book from Deribit"""
    message = create_order_book_message(instrument)
    data = requests.get(message).json()
    bid_assigned = False
    ask_assigned = False
    bid_price = None
    bid_size = None
    ask_price = None
    ask_size = None

    mid_price = data['result']['mark_price']
    mid_volatility = data['result']['mark_iv']
    timestamp = data['result']['timestamp']
    underlying_spot = data['result']['underlying_price']

    expiry = get_expiry_from_ticker(instrument)
    call_or_put = get_call_put_from_ticker(instrument)
    strike = float(instrument.split('-')[2])

    order = OrderBook(instrument, mid_volatility,
                      timestamp, expiry, strike, call_or_put, underlying_spot)
    return order


def get_orders(crypto):
    """ retrieve all order books per crypto"""
    i = 0
    MAX_OPTION_NUMBER = 400

    options = get_options(crypto)
    orders = []

    for index, row in options.iterrows():
        trade_id = row['instrument_name']
        # get_trades_and_compute_implied_mid(trade_id)
        orders.append(get_order_book(trade_id))
        i = i + 1
        if i > MAX_OPTION_NUMBER:
            break

    orders_frame = pd.DataFrame.from_records([order.to_dict() for order in orders])
    date = datetime.datetime.now().strftime("%Y_%m_%d__%H_%M")
    orders_frame.drop(['date'], axis=1, inplace=True)
    orders_frame['DatVal'] = pd.to_datetime("today").strftime("%m/%d/%Y")
    orders_frame['Maturity'] = pd.to_datetime(orders_frame['expiry']) - pd.to_datetime(orders_frame['DatVal'])
    return (orders_frame)


def select_option(days, instrument):
    option_selection = \
        get_orders(instrument)[(get_orders(instrument)['Maturity'] == days)]
    #display(option_selection)
    #option_selection.to_csv(crypto + '_' + '_orders_history.csv', index=False)
    return option_selection


# daily task to get/store vol points on options
# first get all option definitions
#CRYPTOS = ['BTC', 'ETH']
#for crypto in CRYPTOS:
 #   select_option('2 days', crypto)

