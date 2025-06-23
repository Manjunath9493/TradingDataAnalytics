from pyspark.sql import SparkSession
from SmartApi import SmartConnect
import pyotp
import pandas as pd
from datetime import datetime
from pyspark.sql.functions import *
from pyspark.sql.functions import col, lit
from pyspark.sql.types import StructType, StructField, DateType, IntegerType, StringType
from pyspark.sql.window import Window

from config.SparkSession import getSparkSession
from config.params import DataRetentionPrd, SourceFormat, SourcePath, TargetFormat, Target_all_data_path, Target_close_90D_ago, Target_previous_day_data, Target_Final_data
from config.configure_adls import configure_adls_oauth

from modules.Read import read
from modules.dmas_calculation import dmas_calculation
from modules.rsi_calculation import rsi_calculation
from modules.rsi_9dma_calculation import rsi_9dma_calculation
from modules.Increamental_Loading import increamental_load
from modules.Dateparts import DateParts
from modules.Closes_90D_ago import Closes_90D_ago
from modules.Three_month_returns_Calculation import Three_month_returns_Calculation
from modules.previous_day_data import previous_day_data

spark = getSparkSession("Calculations")
configure_adls_oauth(spark)
df = read(SourceFormat,SourcePath)

# Calculation Functions
df1_dmas = dmas_calculation(df)
df2_rsi = rsi_calculation(df1_dmas)
df3_9ma_rsi = rsi_9dma_calculation(df2_rsi)
df4_dateparts = DateParts(df3_9ma_rsi)
df5_Closes_90D_ago = Closes_90D_ago(df4_dateparts)
df5_3Month_returns = Three_month_returns_Calculation(df4_dateparts, df5_Closes_90D_ago)
df6_previous_day_data = previous_day_data(df4_dateparts)

# Loading Incrementally
increamental_load(Target_all_data_path, TargetFormat, df4_dateparts)
increamental_load(Target_close_90D_ago, TargetFormat, df5_Closes_90D_ago)
increamental_load(Target_Final_data, TargetFormat, df5_3Month_returns)
increamental_load(Target_previous_day_data, TargetFormat, df6_previous_day_data)

# Datacleanup: deleting year old rows from table
dataCleanup(DataRetentionPrd, Target_all_data_path, TargetFormat)
dataCleanup(DataRetentionPrd, Target_close_90D_ago, TargetFormat)
dataCleanup(DataRetentionPrd, Target_previous_day_data, TargetFormat)
dataCleanup(DataRetentionPrd, Target_Final_data, TargetFormat)