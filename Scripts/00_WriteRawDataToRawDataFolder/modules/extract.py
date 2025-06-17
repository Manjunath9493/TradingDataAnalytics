def GetHistoricalOHLCData(symbol,symbol_token, interval, from_date, to_date):
    historicParam = {
        "exchange": "NSE",
        "symboltoken": symbol_token,
        "interval": interval,
        "fromdate": from_date,
        "todate": to_date
    }
    try:
        candles = obj.getCandleData(historicParam)['data']
        #print("Raw candle data:", candles)

        df = pd.DataFrame(candles)
        df.columns = ['datetime', 'open', 'high', 'low', 'close', 'volume']
        df['datetime'] = pd.to_datetime(df['datetime'])
        # print(df.head())
        df["Symbol"] = symbol
    except Exception as e:
        print("Error fetching data:", e)
    return df
