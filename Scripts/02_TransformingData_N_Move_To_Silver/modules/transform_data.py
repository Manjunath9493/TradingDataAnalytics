def Transform_Data(df):
    
    return df.withColumn("Date",col("Date").cast("date")).withColumn("symbol",regexp_replace(col("symbol"),"-EQ$",""))

