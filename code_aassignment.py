import requests, json
import pandas as pd
import time

# pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)

max_retries = 3
retry_delay = 10


def getCandleData(time_frame, symbol):
    """
    Get the Candle data for provided time_frame<1m> and symbol.
    :param time_frame:
    :param symbol:
    :return:
    """
    for attempt in range(max_retries):
        try:
            base_url = "https://api.gemini.com/v2"
            response = requests.get(base_url + "/candles/" + symbol + "/" + time_frame)
            response.raise_for_status()
            btc_candle_data = response.json()
            df = pd.DataFrame(btc_candle_data, columns=['time', 'open_price', 'high_price', 'low_price', 'close_price', 'volume'])
            df['candle_open_time'] = pd.to_datetime(df['time'], unit='ms')
            df['candle_close_time'] = df['candle_open_time'] + pd.Timedelta(minutes=1)
            df['date'] = df['candle_open_time'].dt.date
            df['hour'] = df['candle_open_time'].dt.hour
            df['minute'] = df['candle_open_time'].dt.minute
            df['trading_pair'] = symbol
            return df
        except requests.exceptions.RequestException as e:
            print("Error occurred (attempt {}/{}): {}".format(attempt + 1, max_retries, e))
            if attempt < max_retries - 1:
                time.sleep(retry_delay)
            else:
                print("The API response is bad after exceeded the max retry limit")


def getCandleMinimumDateTime(time_frame, symbol):
    """
    Get the minimum timestamp to use to get the respective Trading data
    :param time_frame:
    :param symbol:
    :return:
    """
    for attempt in range(max_retries):
        try:
            base_url = "https://api.gemini.com/v2"
            response = requests.get(base_url + "/candles/" + symbol + "/" + time_frame)
            response.raise_for_status()
            btc_candle_data = response.json()
            return btc_candle_data[-1][0]
        except requests.exceptions.RequestException as e:
            print("Error occurred (attempt {}/{}): {}".format(attempt + 1, max_retries, e))
            if attempt < max_retries - 1:
                time.sleep(retry_delay)
            else:
                print("The API response is bad after exceeded the max retry limit")


# print(candleMinimumDateTime("1m"))


def getTradeDataFrame(start_time, symbol):
    """
    Get the trading data for the provided input symbol and start timestamp.
    :param start_time:
    :param symbol:
    :return:
    """
    output = []
    loop_flag = "Y"
    base_url = "https://api.gemini.com/v1"
    for attempt in range(max_retries):
        try:
            while loop_flag == "Y":
                response = requests.get(base_url + "/trades/" + symbol + "?timestamp=" + str(start_time) + " + &limit_trades=500")
                response.raise_for_status()
                btcusd_trades = response.json()
                if len(btcusd_trades) == 0:
                    loop_flag = "N"
                output.append(btcusd_trades)
                start_time = btcusd_trades[0]["timestampms"]
            df = pd.DataFrame(output)
            df['datetime_utc'] = pd.to_datetime(df['timestampms'], unit='ms')
            df['date'] = df['datetime_utc'].dt.date
            df['hour'] = df['datetime_utc'].dt.hour
            df['minute'] = df['datetime_utc'].dt.minute
            df['usd_vol'] = pd.to_numeric(df.price) * pd.to_numeric(df.amount)
            return df
        except requests.exceptions.RequestException as e:
            print("Error occurred (attempt {}/{}): {}".format(attempt + 1, max_retries, e))
            if attempt < max_retries - 1:
                time.sleep(retry_delay)
            else:
                print("The API response is bad after exceeded the max retry limit")


# print(getTradeDataFrame(candleMinimumDateTime("1m", "btcusd"), "btcusd"))


def rollupTradeData(df):
    """
    Rollup the trading data to get required columns
    :param df:
    :return:
    """
    aggregated_data = df.groupby(['date', 'hour', 'minute']).agg(
        btc_volume=('amount', 'sum'),
        usd_volume=('usd_vol', 'sum'),
        trade_count=('tid', 'count')
    )
    return aggregated_data


def createFinalOutputDataFrame(time_frame, symbol):
    """
    Final function to create desired output dataaframe
    :param time_frame:
    :param symbol:
    :return:
    """
    candle1mdf = getCandleData(time_frame, symbol)
    candlestartdatetime = getCandleMinimumDateTime(time_frame, symbol)
    tradedatadf = getTradeDataFrame(candlestartdatetime, symbol)

    joincandleandtradedf = pd.merge(candle1mdf, tradedatadf, on=['date', 'hour', 'minute'], how='left')
    selectedColumns = joincandleandtradedf[['trading_pair', 'open_price', 'close_price', 'high_price', 'low_price', 'btc_volume', 'usd_volume', 'trade_count', 'candle_open_time', 'candle_close_time']]

    return selectedColumns


def writeDataToFileSystem(time_frame, symbol):
    """
    Write data to file system
    :param time_frame:
    :param symbol:
    :return:
    """
    df = createFinalOutputDataFrame(time_frame, symbol)
    df.to_parquet('desired_output_data.parquet', partition_cols=['trading_pair'])
    print("Desired output has written to file system")
