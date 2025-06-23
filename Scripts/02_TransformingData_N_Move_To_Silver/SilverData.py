from pyspark.sql import SparkSession
import pandas as pd
from datetime import datetime
from pyspark.sql.functions import *
from pyspark.sql.functions import col, lit
from pyspark.sql.types import StructType, StructField, DateType, IntegerType, StringType
from pyspark.sql.window import Window
from pyspark.sql.functions import regexp_replace

from config.SparkSession import getSparkSession
from config.params import DataRetentionPrd, sourcePath, dataFormat, silver_data_path
from config.configure_adls import configure_adls_oauth

from modules.Read import read
from modules.transform_data import Transform_Data
from modules.Increamental_Loading import increamental_load
from modules.DataCleanup import dataCleanup

spark = getSparkSession("Transform_Data")
configure_adls_oauth(spark)
df = read(dataFormat,sourcePath)
print(f"{df.count()} rows read successfully")

transformed_df = Transform_Data(df)
print("Removed 'EQ' from stock name")

LoadingPath = silver_data_path
LoadFormat = dataFormat
loadingDF = transformed_df
increamental_load(LoadingPath, LoadFormat, loadingDF)
print("Loaded transformed data successfully")

dataCleanup(DataRetentionPrd, LoadingPath, LoadFormat)
print("Removed data older than 365 days")

