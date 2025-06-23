from pyspark.sql.window import Window        
from pyspark.sql.functions import *
def Three_month_returns_Calculation(df8,df10):
    """
    Here to calculate 3 month old date, followed below steps
    1. substracted 90 days from todays date (assumed 30 days per month)
    2. considered 5 days range before 90 days, meaning taking 90-95 days old range data to avoid falling in market holidays or weekends.
    3. within this 5 days range, i am considering max available date in the dataset to get 3 month old close
    
    We have current day close and 3 month old close, using these i am calculating 3 month returns using below formula
    returns = ((currentDayClose - 3_month_old_close)/3_month_old_close) * 100
    """
    try:
        w5 = Window.partitionBy("Symbol").orderBy(col("date").desc())
        df11 = df8.withColumn("RN", row_number().over(w5)).filter(col("RN")==1).\
        drop("RN","Open","Low","High","PreviousDayClose","Change","Gain","Loss","avgGain","avgLoss","RS")

        df14 = df11.join(df10, on="Symbol", how="inner")
        df15 = df14.withColumn("3MonthChnage", (col("close")-col("close_90D"))).\
        withColumn("3_Month_returns_in_%", round((col("3MonthChnage")/col("close_90D"))*100,2))
        df16 = df15.drop("close_90D","90D_Close_date","3MonthChnage").dropDuplicates()
        return df16
    except Exception as e:
        print(f"An error occured while calculating 3 month returns: {e}")
 