from pyspark.sql.types import *
from pyspark.sql import Row
from datetime import datetime
from config.SparkSession import getSparkSession
spark = getSparkSession("QualityCheck")
def QualityCheckLogging(df,Logpath,Logformat,Logmode):
    try:
        Zero_or_negative_valued_rows = df.filter((col("Open")<=0) | (col("High")<=0) | (col("Close")<=0) | (col("Low")<=0) | (col("Volume")<=0)).count()  
        
        invalid_records= df.filter((col("High")<col("low")) | (col("Open") > col("High")) | (col("Open") < col("Low")) | (col("Close") > col("High")) | (col("Close") < col("Low"))).count()
        
    except Exception as e:
        print(f"An Error occured during quality check: {e}")
    
    try:        
        schema = StructType([
            StructField("run_date", StringType(), True),
            StructField("step_name", StringType(), True),
            StructField("total_rows", IntegerType(), True),
            StructField("null_Open", IntegerType(), True),
            StructField("null_Close", IntegerType(), True),
            StructField("null_low", IntegerType(), True),
            StructField("null_high", IntegerType(), True),
            StructField("invalid_rows", IntegerType(), True),
            StructField("Duplicates", IntegerType(), True),
            StructField("Zero_or_negative_valued_rows", IntegerType(), True)
        ])
        
        log_row = Row(
            run_date = datetime.today().strftime('%Y-%m-%d'),
            step_name = "01_QualityCheck_Move_To_Bronze",
            total_rows = df.count(),
            null_Open = df.filter(col("Open").isNull()).count(),
            null_Close = df.filter(col("Close").isNull()).count(),
            null_low = df.filter(col("Low").isNull()).count(),
            null_high = df.filter(col("High").isNull()).count(),
            invalid_rows = invalid_records,
            Duplicates = df.count() - df.dropDuplicates().count(),
            Zero_or_negative_valued_rows = Zero_or_negative_valued_rows
        ) 
        
        log_df = spark.createDataFrame([log_row], schema)
    
    except Exception as e:
        print(f"An error occured during logging: {e}")
        
    try:
        path = Logpath
        writeformat = Logformat
        writemode = Logmode
        
        write(path,writeformat,writemode)
    
    except Exception as e:
        print(f"An error occured while writing log to adls: {e}")
    
