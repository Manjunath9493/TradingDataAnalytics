def ForLoopForStockData(SymToken_Symbol_pairs,interval,FromDate_formatted,ToDate_formatted):
    for token, symbol in SymToken_Symbol_pairs.items():
        DailyData = GetHistoricalOHLCData(symbol,token,interval,FromDate_formatted,ToDate_formatted)
        my_df = pd.DataFrame(DailyData)
        spark_df = spark.createDataFrame(my_df)
        final_df = final_df.union(spark_df )
    return final_df