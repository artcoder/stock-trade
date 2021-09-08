#
# A framework to backtest and run stock trading strategies for a portfolio
#
# David Guilbeau

import sqlite3
import datetime
from datetime import timedelta
import yfinance as yf
import pickle
import pandas as pd
import csv

# Parameters
download = True
database_filename = r'.\stock_data.sqlite3'
pickle_filename = r'.\stock_group_df_0.0.0.pkl'
symbols_filename = r'.\sp500symbols.csv'

window_length = 15

stock_list = []
csvfile = open(symbols_filename, newline='')
reader = csv.reader(csvfile)
for row in reader:
    stock_list.append(row[0])


maximum_trading_days_needed = 2200
maximum_calendar_days_needed = maximum_trading_days_needed * 365.25 / 253
# 253 trading days in a year
# 365.25 days in a year


# Set requested date range
finish_date = datetime.date.today()
# finish_date = datetime.datetime(2021, 7, 6)
start_date = finish_date - timedelta(days=maximum_calendar_days_needed)
print("Requested start:", start_date, "finish:", finish_date)

extra_days = 5  # extra days to look at in case the start date is not a trading day


def create_database_if_needed():
    global cur

    # If table does not exist, create it
    sql = '''
    CREATE TABLE  IF NOT EXISTS stock_data
    (date timestamp NOT NULL,
    ticker text NOT NULL,
    open real,
    high real,
    low real,
    close real,
    "Adj Close" real,
    volume real,
    primary key(date, ticker)
    )
    '''
    cur.execute(sql)


def find_download_start_date(requested_start_date):
    # print("In find_download_start_date:", requested_start_date, type(requested_start_date))
    global cur

    # Find the last date in the database:
    sql = '''
    Select date From stock_data
    Order By date Desc
    Limit 1
    '''
    cur.execute(sql)
    rows = cur.fetchall()

    # if no date
    if len(rows) < 1:
        print('No rows found in database table.')
        download_start_date = requested_start_date
    else:
        print('Last date found in database:', rows[0][0])
        # Download the day after the one in the database
        download_start_date = rows[0][0].date() + timedelta(days=1)

    return download_start_date


# downloads stock data to the database
def download_stock_data(download_start_date, download_finish_date):
    global stock_list
    global cur

    if download:
        print('Downloading...')
        data = yf.download(stock_list,
                           start=(download_start_date - timedelta(days=extra_days)),
                           end=(download_finish_date + timedelta(days=1)),
                           group_by='ticker')

        # save to a pickle file to make it possible work around network issues
        data.to_pickle(pickle_filename)
    else:
        pickle_file = open(pickle_filename, 'rb')
        data = pickle.load(pickle_file)
        # todo: restrict the dataframe to (download_start_date, download_finish_date)
        # so that data isn't added twice

    # https://stackoverflow.com/questions/63107594/how-to-deal-with-multi-level-column-names-downloaded-with-yfinance/63107801#63107801
    t_df = data.stack(level=0).rename_axis(['Date', 'Ticker']).reset_index(level=1)
    t_df = t_df.reset_index()

    print('Inserting data into database...')
    # This would insert dataframe data into database, but it fails if a date and ticker already exist
    try:
        t_df.to_sql('stock_data', con, if_exists='append', index=False)

    except sqlite3.IntegrityError:
        print("Could not insert all data at once.")

        for i in range(len(t_df)):

            sql = 'insert into stock_data (date, ticker, close, high, low, open, volume) ' \
                  'values (?,?,?,?,?,?,?)'
            try:
                cur.execute(sql, (t_df.iloc[i].get('Date').to_pydatetime(),
                                  t_df.iloc[i].get('Ticker'),
                                  t_df.iloc[i].get('Adj Close'),
                                  t_df.iloc[i].get('High'),
                                  t_df.iloc[i].get('Low'),
                                  t_df.iloc[i].get('Open'),
                                  t_df.iloc[i].get('Volume')))
                print("\r", i, t_df.iloc[i].get('Ticker'), end='')

            except sqlite3.IntegrityError:
                print("\r", "Failed inserting:", str(t_df.iloc[i][0]), t_df.iloc[i][1], end='')

        con.commit()
        print("\r                                                    ")
#


def download_to_database():

    global cur

    create_database_if_needed()

    download_start_date = find_download_start_date(start_date)

    download_finish_date = finish_date

    if download_start_date <= download_finish_date:
        download_stock_data(download_start_date, download_finish_date)
    else:
        print("Not downloading.")


def get_from_database():

    global cur

    # Debug output
    # Find actual start date
    query = '''
    select date from stock_data
    order by date
    limit 1
    '''
    cur.execute(query)
    t = cur.fetchone()
    print("Database start date:", t[0])

    # Debug output
    # Find actual finish date
    query = '''
    Select date From stock_data
    Order By date Desc
    limit 1
    '''
    cur.execute(query)
    t = cur.fetchone()
    print("Database finish date:", t[0])

    # Load the database table into a dataframe
    sql = '''
       Select date, ticker, close From stock_data
       Order By date Asc
       '''
    cur.execute(sql)

    input_df = pd.DataFrame(cur.fetchall(),
                            columns=['date', 'ticker', 'close'])
    return input_df
#


def calculate_allocation(window_df):
    pass


def calculate_forward_return(future_df, allocation):
    pass


if __name__ == '__main__':

    # detect_types= is for timestamp support
    con = sqlite3.connect(database_filename,
                          detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES)
    cur = con.cursor()

    download_to_database()

    # load a dataframe with stock data from the database that will be needed for the run
    all_input_df = get_from_database()

    con.close()

    ####
    # back test

    # window_start and window_finish are in days relative to dataframe rows
    window_start = 0
    window_finish = window_start + window_length
    end_of_stock_df = len(all_input_df)
    print('end_of_stock_df:', end_of_stock_df)

    running_return = \
        {stock_list[0]: 1,
         stock_list[1]: 1,
         'portfolio': 1}

    window_df_length = len(all_input_df.iloc[0])

    quit_loop = False
    first_loop = True
    while True:
        print('---')

        window_df = all_input_df.iloc[window_start * window_df_length:window_finish * window_df_length]
        print('past window:', window_df.iloc[0]['date'], window_df.iloc[-1]['date'])
        window_df = window_df.set_index(['ticker', 'date']).sort_index()

        allocation = calculate_allocation(window_df)

        # make future_df be from the end of stock_df to 5 days after that
        future_df = all_input_df.iloc[(window_finish - 1) * window_df_length:(window_finish - 1 + 5) * window_df_length]

        if first_loop:
            entire_future_df = all_input_df.iloc[(window_finish - 1) * window_df_length:]
            first_loop = False

        print('future window:', future_df.iloc[0]['date'], future_df.iloc[-1]['date'])
        # print('future_df:', future_df)
        future_df = future_df.set_index(['ticker', 'date']).sort_index()

        # show the return compared to the component investments
        actual_return = calculate_forward_return(future_df, allocation)
        print('portfolio:', round(actual_return['portfolio'], 3))

        running_return['portfolio'] = running_return['portfolio'] * (1 + actual_return['portfolio'])

        window_finish = window_finish + 4
        window_start = window_start + 4

        if quit_loop:
            break

        # todo: update MAGIC NUMBER: 2 is the length of stock_list
        if window_finish > end_of_stock_df / 2:
            break
            # print('at the end of the database data')
            # put the end of the window at the end of the stock data frame
            window_finish = int(end_of_stock_df / 2)
            window_start = window_finish - trading_days_window
            quit_loop = True
    #

    # loop:

        # get a subset of the data as input for a prediction
        # window = all_input_df[...]

        # if this is not the first time through the loop:
            # calculate the return of the previous output on this subset

        # give the input to a prediction function

        # get the output of the prediction function
