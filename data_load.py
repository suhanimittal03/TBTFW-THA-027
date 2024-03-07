import psycopg2 as pg2 
import os, sys
import pandas as pd
import glob
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from tabulate import tabulate

conn = pg2.connect(database="db_name", user='user_name', password='password', host="localhost", port=5432)

cursor = conn.cursor()

cursor.execute("select version()")

companies = ['tsla', 'inr_x', 'tatamotors', 'aapl', 'hdb', 'jiofin', 'mara']
for comp in companies:
    sql = f"create table {comp} (Date Date, Open varchar(50), High varchar(50), Low varchar(50), Close varchar(50), Adj_close varchar(50), Volume varchar(50));"
    cursor.execute(sql)
# sql2= f"\COPY AAPL(Date, Open, high, Low, Close, Adj_close, Volume) from '/home/suhani/Documents/TBTFW-THA-027/data/AAPL.csv' DELIMITER ',' CSV HEADER;"

path = "/home/suhani/Documents/TBTFW-THA-027/data/*.csv"
filepath = []
for fname in glob.glob(path):
    # print(fname)
    filepath.append(fname)
    df1 = pd.read_csv(fname)
    df = df1.dropna()
    # make new clean csv files or it makes no sense
i=0
for comp, pathn in zip(companies, filepath):
    # print(comp, pathn)
    with open(pathn, 'r') as f:
        # print(f'_____________________{i}')
        next(f)
        cursor.execute(f'delete from {comp};')
        cursor.copy_from(f, comp, sep=',', columns=('date', 'open', 'high', 'low', 'close', 'adj_close', 'volume'))
        cursor.execute(f'select * from {comp};')
        data = cursor.fetchall()
        # print(comp, data[0])
        i+=1
    

stocks_data={}
for comp in companies:
    cursor.execute(f'select * from {comp};')
    data = cursor.fetchall()
    stocks_data[comp] = data

stocks_df={}
for stock, values in stocks_data.items():
    df1 = pd.DataFrame(values, columns=['date','open','high','low','close', 'adj_close', 'volume'])
    df = df1.dropna()
    stocks_df[stock] = df

    cols_to_convert = ['open', 'high', 'low', 'close', 'adj_close', 'volume']
    stocks_df[stock][cols_to_convert] = stocks_df[stock][cols_to_convert].apply(pd.to_numeric, errors='coerce')




def crossoverstrategey(strategy, stock, stocks_df, short_window=20, long_window=200):
    # close_price = stocks_df[stock]['close']
    start_date = stocks_df[stock]['date'].iloc[0]
    end_date = stocks_df[stock]['date'].iloc[-1]

    short_wd = str(short_window) + '_day_moving_avg'
    long_wd = str(long_window) + '_day_moving_avg'

    if strategy == 'SMA':
        stocks_df[stock][short_wd] = stocks_df[stock]['close'].rolling(window=short_window, min_periods=1).mean()
        stocks_df[stock][long_wd] = stocks_df[stock]['close'].rolling(window=long_window, min_periods=1).mean() 

    # generate the signal
    stocks_df[stock]['signal'] = 0
    stocks_df[stock]['signal'] = np.where(stocks_df[stock][short_wd] > stocks_df[stock][long_wd], 1, 0)

    stocks_df[stock]['position'] = stocks_df[stock]['signal'].diff()

    plt.figure(figsize=(20,10))
    plt.plot(stocks_df[stock]['date'], stocks_df[stock]['close'], label='Close', color='black', lw=1)

    # Plot smoothed moving averages
    plt.plot(stocks_df[stock]['date'], stocks_df[stock]['20_day_moving_avg'], label='20-day SMA', color='blue', lw=2)
    plt.plot(stocks_df[stock]['date'], stocks_df[stock]['200_day_moving_avg'], label='200-day SMA', color='purple', lw=2)


    plt.plot(stocks_df[stock][stocks_df[stock]['position'] == 1].index, 
                stocks_df[stock][short_wd][stocks_df[stock]['position'] == 1], 
                '^', markersize = 10, color = 'g', alpha = 1.0, label = 'buy')

        # plot 'sell' signals
    plt.plot(stocks_df[stock][stocks_df[stock]['position'] == -1].index, 
            stocks_df[stock][short_wd][stocks_df[stock]['position'] == -1], 
            'v', markersize = 10, color = 'r', alpha = 1.0, label = 'sell')

    # Set plot title and labels
    plt.title(f'{comp} Closing Prices and {short_window} and  {long_window} Moving Averages')
    plt.xlabel('Date')
    plt.ylabel('Price')
    # plt.grid(True)

    # Format date axis
    plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m'))
    plt.gca().xaxis.set_major_locator(mdates.MonthLocator(interval=2))
    plt.gcf().autofmt_xdate()

    # Adjust legend position
    plt.legend(loc='upper left')
    plt.xlim(start_date, end_date)
    plt.show()
    # plt.savefig(f'{stock} {short_window} and  {long_window} Moving Averages Crossover.png')

    df_pos = stocks_df[stock][(stocks_df[stock]['position'] == 1) | (stocks_df[stock]['position'] == -1)]
    df_pos['position'] = df_pos['position'].apply(lambda x: 'Buy' if x == 1 else 'Sell')
    print(tabulate(df_pos, headers = 'keys', tablefmt = 'psql'))

strategy='SMA'
for comp in companies:
    crossoverstrategey(strategy, comp, stocks_df)


def calculate_trade_profit_loss(df, stock):
    # Calculate the profit/loss for each trade based on the position (Buy/Sell)
    df[stock]['profit_loss'] = df[stock]['position'].diff() * df[stock]['close'].shift(-1)

# Define a function to calculate overall profit/loss for each stock
def calculate_overall_profit_loss(df, stock):
    # Calculate the overall profit/loss for each stock by summing up the profits and losses
    overall_profit_loss = df[stock]['profit_loss'].sum()
    return overall_profit_loss

for stock in companies:
    # Calculate profit/loss for each trade
    calculate_trade_profit_loss(stocks_df, stock)
    
    # Calculate overall profit/loss for the stock
    overall_profit_loss = calculate_overall_profit_loss(stocks_df, stock)
    # cursor.execute(f"ALTER TABLE {stock} ADD COLUMN IF NOT EXISTS overall_profit_loss float;")
    # # Store the results in the database
    # cursor.execute(f"INSERT INTO {stock} (overall_profit_loss) VALUES ({overall_profit_loss})")




conn.commit()
conn.close()

