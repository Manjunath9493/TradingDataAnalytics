from pyspark.sql.window import Window
from pyspark.sql.functions import *
def rsi_9dma_calculation(df5):
    try:
        # Calculating the 9MA on RSI 
        w3 = Window.partitionBy("Symbol").orderBy(col("Date").desc()).rowsBetween(0,8)
        df6 = df5.withColumn("RSI_9MA",round(avg("RSI").over(w3),2))
        return df6
    except Exception as e:
        print(f"An error occured while calculating 9MA on RSI: {e}")