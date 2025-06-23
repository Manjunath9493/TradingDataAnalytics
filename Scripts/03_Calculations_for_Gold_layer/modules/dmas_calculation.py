from pyspark.sql.window import Window
from pyspark.sql.functions import format_number
from pyspark.sql.functions import avg
def dmas_calculation(df):
    try:
        # DMA Calculation
        window_9 = Window.partitionBy("Symbol").orderBy("Date").rowsBetween(-8,0)
        df1 = df.withColumn("9Dma", format_number(avg("close").over(window_9),2))
        
        window_20 = Window.partitionBy("Symbol").orderBy("Date").rowsBetween(-19,0)
        df1 = df1.withColumn("20Dma", format_number(avg("close").over(window_20),2))
    
        window_50 = Window.partitionBy("Symbol").orderBy("Date").rowsBetween(-49,0)
        df1 = df1.withColumn("50Dma", format_number(avg("close").over(window_50),2))
        
        window_200 = Window.partitionBy("Symbol").orderBy("Date").rowsBetween(-199,0)
        df1 = df1.withColumn("200Dma", format_number(avg("close").over(window_200),2))
        
        return df1
    except Exception as e:
        print(f"An error occured while calculating dmas: {e}")
 