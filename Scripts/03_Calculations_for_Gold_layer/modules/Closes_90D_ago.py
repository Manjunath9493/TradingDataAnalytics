from datetime import date, timedelta, datetime
from pyspark.sql.window import Window

def Closes_90D_ago(df8):    
    try:
        date_90D_ago_upper =  date.today()-timedelta(days=90)
        if date_90D_ago_upper.weekday() == 5:
            date_90D_ago_upper =  date_90D_ago_upper-timedelta(days=1)
        elif date_90D_ago_upper.weekday() == 6:
            date_90D_ago_upper =  date_90D_ago_upper-timedelta(days=2)
        
        date_90D_ago_lower = date_90D_ago_upper-timedelta(days=5)
    except Exception as e:
        print(f"An error occured while 3 month range: {e}")
 
    try:
        df9 = df8.filter((col("date")>=date_90D_ago_lower) & (col("date")<=date_90D_ago_upper))
        w4 = Window.partitionBy("Symbol").orderBy(col("date").desc())
        df10 = df9.withColumn("RN",row_number().over(w4)).filter(col("RN")==1).drop("RN")
        df10 = df10["symbol","date","close"].withColumnRenamed("close","close_90D").withColumnRenamed("date","90D_Close_date")
        return df10
    except Exception as e:
        print(f"An error occured while calculating 3 month returns: {e}")