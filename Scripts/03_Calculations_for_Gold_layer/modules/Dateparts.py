def DateParts(df6):   
    try:
        # Adding Year, Month and Date columns for analytical purposes
        df7 = df6.withColumn("day", dayofmonth(col("date"))).\
            withColumn("month",month(col("date"))).\
                withColumn("year",year(col("date")))
        df8 = df7.dropDuplicates()
        return df8 
        
    except Exception as e:
        print(f"An error occured while adding Year, Month and Date columns: {e}")  