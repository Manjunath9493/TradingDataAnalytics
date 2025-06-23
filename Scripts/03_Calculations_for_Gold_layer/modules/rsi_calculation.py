from pyspark.sql.window import Window
from pyspark.sql.functions import avg, lead, col, round, when
from pyspark.sql.functions import *
def rsi_calculation(df1):
    try:
        # RSI Calculation
        # Previous day close
        w1 = Window.partitionBy("Symbol").orderBy(col("Date").desc())
        df2 = df1.withColumn("PreviousDayClose", lead(col("close"), 1).over(w1))
        
        # Change, Gain, Loss
        df3 = df2.withColumn("Change",round((col("close")-col("PreviousDayClose")),2)).\
            withColumn("Gain",when(col("Change")>=0,col("Change")).otherwise(0)).\
                withColumn("Loss",when(col("Change") < 0,col("Change")).otherwise(0))
        
        # RSI is calculated for 14 period
        # avgGain, avgLoss
        w2 = Window.partitionBy("Symbol").orderBy(col("Date").desc()).rowsBetween(0,13)
        df4 = df3.withColumn("avgGain",round(abs(avg("Gain").over(w2)),2)).\
            withColumn("avgLoss",round(abs(avg("Loss").over(w2)),2))            
        
        # RS: Relative strength calculated by deviding avg_gain with avg_loss
        # RSI: Relative strength index is calculated by using this formula -->  100-(100/(1 + RS))
        df5 = df4.withColumn("RS",round(when(col("avgLoss")!=0.0, (col("avgGain")/col("avgLoss"))).otherwise(0),2)).\
            withColumn("RSI",round(when(col("avgLoss")==0.0, 100).otherwise((100-(100/(1+col("RS"))))),2))
        return df5
    except Exception as e:
        print(f"An error occured while calculating RSI: {e}")
 