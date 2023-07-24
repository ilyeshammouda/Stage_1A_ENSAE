import pandas as pd
nasdaq=pd.read_excel('nasdaq_prices.xlsx')
nasdaq['percentage'] = nasdaq['Adj Close'].pct_change()
nasdaq_pct=nasdaq['percentage'].iloc[1:]
nasdaq['historical_volatility']= (nasdaq_pct.rolling(30).std().dropna())*100
nasdaq.to_excel("historical_volatility).xlsx")