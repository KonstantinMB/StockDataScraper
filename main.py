import requests
import pandas as pd
import time
import datetime
from dotenv import load_dotenv
import os

# Load environment variables from .env
load_dotenv()

# Retrieve the API key from the environment
api_key = os.getenv("POLYGON_API_KEY")

if api_key is None:
    raise ValueError("You haven't specified Polygon POLYGON_API_KEY in .env file. " +
                     "To do so, create a .env file in the projects directory and add the key in teh following format: POLYGON_API_KEY=<YOUR-API_KEY>")

# URL for extracting the stock info
aggregate_urls = 'https://api.polygon.io/v2/aggs/ticker/'
details_url = 'https://api.polygon.io/v3/reference/tickers/'

# Date of candle for aggregates
date_of_candle_aggregates = '2020-10-14/2020-10-14'

# Date of candle for ticket details
date_of_candle_tickets = '2020-10-14'

# Candle type can be: minute, hour, day, week, month, quarter, year
candle_type = 'hour'


def get_all_symbols():
    symbols = pd.read_csv('symbols.csv')
    symbols = symbols['Symbols'].tolist()
    return symbols


def get_url_aggregates():
    list_of_symbols = get_all_symbols()
    list_of_urls = []
    for i in range(0, len(list_of_symbols)):
        final_url = aggregate_urls + list_of_symbols[
            i] + '/range/1/' + candle_type + '/' + date_of_candle_aggregates + '?adjusted=true&sort=asc&apiKey=' + api
        list_of_urls.append(final_url)
    return list_of_urls


def get_url_ticker_info():
    list_of_symbols = get_all_symbols()
    list_of_urls = []
    for i in range(0, len(list_of_symbols)):
        final_url = details_url + list_of_symbols[i] + '?date=' + date_of_candle_tickets + '&apiKey=' + api
        list_of_urls.append(final_url)
    return list_of_urls


def ticket_info(list_urls):
    list_of_tickers = []
    list_of_shares = []
    for k in range(0, len(list_urls)):
        if k % 5 == 0 and k != 0:
            time.sleep(65)
        try:
            data_from_web = requests.get(list_urls[k]).json()
            results = data_from_web['results']

            list_of_tickers.append(results['ticker'])
            list_of_shares.append(results['weighted_shares_outstanding'])

        except Exception as e:
            print(e)
    data = {"ticker": list_of_tickers, "outstanding_shares": list_of_shares}
    return data


def aggregates(list_urls):
    list_prev_close_price = []
    list_today_open_price = []
    list_highs = []
    list_lows = []
    list_gaps = []
    list_close = []
    list_volume = []
    list_highs_time = []
    list_lows_time = []
    list_pre_market_volumes = []
    list_pre_market_highs = []
    list_pre_market_highs_time = []
    list_pre_market_lows_after_highs = []

    for k in range(0, len(list_urls)):
        # In order to retrieve free data from polygon
        if k % 5 == 0:
            time.sleep(65)
        try:
            # Getting the request from Polygon.io API:
            data = requests.get(list_urls[k]).json()
            results = data['results']

            # Getting all market day AND pre-market data.
            market_open_price = 0
            market_close = {}
            market_high = {}
            market_low = {}
            market_volume = {}

            pre_market_close = {}
            pre_market_high = {}
            pre_market_low = {}
            pre_market_volume = {}
            for m in range(16):
                res_dic = results[m]
                if m == 0:
                    market_open_price = res_dic['o']
                    list_today_open_price.append(market_open_price)
                if m == 15:
                    list_close.append(res_dic['c'])
                if m < 6:
                    pre_market_close[res_dic['c']] = res_dic['t']
                    pre_market_high[res_dic['h']] = res_dic['t']
                    pre_market_low[res_dic['l']] = res_dic['t']
                    pre_market_volume[res_dic['v']] = res_dic['t']

                market_close[res_dic['c']] = res_dic['t']
                market_high[res_dic['h']] = res_dic['t']
                market_low[res_dic['l']] = res_dic['t']
                market_volume[res_dic['v']] = res_dic['t']

            # The highest market day price:
            highest_market_price = max(list(market_high.keys()))
            list_highs.append(highest_market_price)

            # The highest PRE-market day price
            highest_pre_market_price = max(list(pre_market_high.keys()))
            list_pre_market_highs.append(highest_market_price)

            # The lowest market day price:
            lowest_market_price = min(list(market_low.keys()))
            list_lows.append(lowest_market_price)

            # TODO
            yesterday_market_close = 0
            list_prev_close_price.append(yesterday_market_close)

            # The GAP between highest and lowest price:
            gap_price = yesterday_market_close / market_open_price * 100
            list_gaps.append(gap_price)

            index = 0
            for m in range(6):
                if list(pre_market_high.keys())[m] == highest_pre_market_price:
                    index = m
                    break

            pm_low_after_high = list(pre_market_low.keys())[index]
            for m in range(index, 6):
                if pm_low_after_high > list(pre_market_low.keys())[m]:
                    pm_low_after_high = list(pre_market_low.keys())[m]
            list_pre_market_lows_after_highs.append(pm_low_after_high)

            # The lowest PRE-market day price
            # lowest_pre_market_price = min(zip(pre_market_low.values(), list(pre_market_low.keys())))[1]

            # The date & hour of the highest MARKET close price
            highest_market_date = datetime.datetime.fromtimestamp(market_high[highest_market_price] / 1000.0,
                                                   tz=datetime.timezone.utc)
            list_highs_time.append(highest_market_date)

            # The date & hour of the highest PRE-MARKET close price
            highest_pre_market_date = datetime.datetime.fromtimestamp(pre_market_high[highest_pre_market_price] / 1000.0,
                                                   tz=datetime.timezone.utc)
            list_pre_market_highs_time.append(highest_pre_market_date)

            # The date & hour of the lowest MARKET price
            lowest_market_date = datetime.datetime.fromtimestamp(market_low[lowest_market_price] / 1000.0,
                                                                  tz=datetime.timezone.utc)
            list_lows_time.append(lowest_market_date)

            # Total volume for the day:
            market_volume = sum(market_volume)
            list_volume.append(market_volume)

            # Volume PRE-market:
            pre_market_volume = sum(pre_market_volume)
            list_pre_market_volumes.append(pre_market_volume)
        except Exception as e:
            print(e)

    return {'Prev Close Price': list_prev_close_price, 'Today Open Price': list_today_open_price,
            'High(h)': list_highs, 'Low(l)': list_lows,'Gap(%)': list_gaps,
            'Close(c)': list_close, 'Volume': list_volume, 'High Time': list_highs_time,
            'Low Time': list_lows_time, 'PM Volume': list_pre_market_volumes,
            'PM High': list_pre_market_highs, 'PM High Time': list_pre_market_highs_time,
            'PM Low After High': list_pre_market_lows_after_highs}
    

if __name__ == '__main__':

    stock_information = aggregates(get_url_aggregates())
    
    # Getting all data into a Panda DataTable
    series_of_stock_information = pd.Series(stock_information)

    # Tranform the dict into a Pandas DataFrame
    df = pd.DataFrame(series_of_stock_information)
    result = df.transpose()
    result = result.rename_axis(None)

    # Save the DF in a new CSV file
    with open('data.csv', 'a', newline='') as f:
        result.to_csv(f, index=False, header=False)
