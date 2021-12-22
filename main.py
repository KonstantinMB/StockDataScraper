import requests
import pandas as pd
import time
import datetime

# Put your Polygon.io API Key in the brackets:
api = 'GDzeXeFuL95oYDxg5DWiNUHwdgSYVujo'

# URL for extracting the stock info
aggregate_urls = 'https://api.polygon.io/v2/aggs/ticker/'
details_url = 'https://api.polygon.io/vX/reference/tickers/'

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
    data = {"ticker": [], "outstanding_shares": []}
    list_of_tickers = []
    list_of_shares = []
    for k in range(0, len(list_urls)):
        if k % 5 == 0 and k != 0:
            time.sleep(65)
        try:
            data_from_web = requests.get(list_urls[k]).json()
            results = data_from_web['results']

            list_of_tickers.append(results['ticker'])
            list_of_shares.append(results['outstanding_shares'])

        except Exception as e:
            print(e)
        # else:
        #     with open('data.csv', 'a', newline='') as f:
        #         result.to_csv(f, index=False, header=False)
    data["ticker"] = list_of_tickers
    data['outstanding_shares'] = list_of_shares
    print(data)


def aggregates(list_urls):
    head = ['Date', 'Ticker', 'Outstanding Shares', 'Prev Close Price', 'Today Open Price', 'High(h)', 'Low(l)'
            'Gap(%)', 'Close(c)', 'Volume', 'High Time', 'Low Time', 'PM Volume', 'PM High', 'PM High Time',
            'PM Low After High']
    head = pd.DataFrame(head).transpose()
    head.to_csv('data.csv', index=False, header=False)

    for k in range(0, len(list_urls)):
        list_of_info = []

        # In order to retrieve free data from polygon
        if k % 5 == 0 and k != 0:
            time.sleep(65)
        try:
            # Getting the request from Polygon.io API:
            data = requests.get(list_urls[k]).json()
            results = data['results']

            if len(results) == 0:
                list_of_info = 'No data provided by the API.'

            # Getting all market day AND pre-market data.
            market_open_price = 0
            market_close_price = 0
            market_close = {}
            market_high = {}
            market_low = {}
            market_volume = {}

            pre_market_close = {}
            pre_market_high = {}
            pre_market_low = {}
            pre_market_volume = {}
            for m in range(1, 17):
                res_dic = results[m]
                if m == 1:
                    market_open_price = res_dic['o']
                if m == 16:
                    market_close_price = res_dic['c']
                if m <= 6:
                    pre_market_close[res_dic['c']] = res_dic['t']
                    pre_market_high[res_dic['h']] = res_dic['t']
                    pre_market_low[res_dic['l']] = res_dic['t']
                    pre_market_volume[res_dic['v']] = res_dic['t']

                market_close[res_dic['c']] = res_dic['t']
                market_high[res_dic['h']] = res_dic['t']
                market_low[res_dic['l']] = res_dic['t']
                market_volume[res_dic['v']] = res_dic['t']

            # The highest market day close price:
            highest_market_price = max(zip(market_close.values(), market_close.keys()))[1]

            # The highest PRE-market day price
            highest_pre_market_price = max(zip(pre_market_close.values(), pre_market_close.keys()))[1]

            # The lowest market day close price:
            lowest_market_price = min(zip(market_close.values(), market_close.keys()))[1]

            # The GAP between highest and lowest price:
            gap_price = (lowest_market_price / highest_market_price) * 100

            index = 0
            for m in range(1, 7):
                if pre_market_high.keys()[m] == highest_pre_market_price:
                    index = m
                    break

            pm_low_after_high = pre_market_low.keys()[index]
            for m in range(index, 7):
                if pm_low_after_high > pre_market_low.keys()[m]:
                    pm_low_after_high = pre_market_low.keys()[m]


            # The lowest PRE-market day price
            lowest_pre_market_price = min(zip(pre_market_close.values(), pre_market_close.keys()))[1]

            # The date & hour of the highest MARKET close price
            highest_market_date = datetime.datetime.fromtimestamp(market_close[highest_market_price] / 1000.0,
                                                   tz=datetime.timezone.utc)

            # The date & hour of the highest PRE-MARKET close price
            highest_pre_market_date = datetime.datetime.fromtimestamp(pre_market_close[highest_pre_market_price] / 1000.0,
                                                   tz=datetime.timezone.utc)

            # The date & hour of the lowest MARKET close price
            lowest_market_date = datetime.datetime.fromtimestamp(market_close[lowest_market_price] / 1000.0,
                                                                  tz=datetime.timezone.utc)

            # # The date & hour of the lowest PRE-MARKET close price
            # lowest_pre_market_date = datetime.datetime.fromtimestamp(
            #     pre_market_close[lowest_pre_market_price] / 1000.0,
            #     tz=datetime.timezone.utc)

            # Total volume for the day:
            market_volume = sum(market_volume)

            # Volume PRE-market:
            pre_market_volume = sum(pre_market_volume)




    #         # Adding all elements in a list, in order to convert it to the table
    #         list_of_info.extend([ticker, close_price, date])
    #
    #         # Getting all data into a Panda DataTable
    #         stock_info = pd.Series(list_of_info)
    #         df = pd.DataFrame(stock_info)
    #
    #         result = df.transpose()
    #         result = result.rename_axis(None)
        except Exception as e:
            print(e)
    #     else:
    #         with open('data.csv', 'a', newline='') as f:
    #             result.to_csv(f, index=False, header=False)
    #
    print('Aggregates data has been loaded.')


if __name__ == '__main__':
    ticket_info(get_url_ticker_info())

