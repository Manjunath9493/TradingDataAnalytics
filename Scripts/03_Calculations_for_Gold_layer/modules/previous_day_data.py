def previous_day_data(df8):
    try:
        df12 = df8.withColumn("RN", row_number().over(w5)).filter(col("RN")==2).\
        drop("Close","Volume","RN","Open","Low","High","PreviousDayClose","Change","Gain","Loss","avgGain","avgLoss","RS")
        df13 = df12
        R_col = ["200Dma","50Dma","20Dma","9Dma","RSI","RSI_9MA"]
        for c in R_col:
            df13 = df13.withColumnRenamed(c,c+"_prev_day")
        return df13
        
    except Exception as e:
        print(f"An error occured while getting previous day data: {e}")