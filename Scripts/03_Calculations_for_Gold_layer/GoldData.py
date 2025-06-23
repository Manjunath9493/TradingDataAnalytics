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
from modules.DataCleanup import dataCleanup

spark = getSparkSession("Calculations")
configure_adls_oauth(spark)
df = read(SourceFormat,SourcePath)
print(f"{df.count()} rows are read from Source")

# Calculation Functions
df1_dmas = dmas_calculation(df)
print(f"DMAs Calculation is successfully completed, Num of rows processed: {df1_dmas.count()}")

df2_rsi = rsi_calculation(df1_dmas)
print(f"RSI Calculation is successfully completed, Num of rows processed: {df2_rsi.count()}")

df3_9ma_rsi = rsi_9dma_calculation(df2_rsi)
print(f"9MA on RSI Calculation is successfully completed, Num of rows processed: {df3_9ma_rsi.count()}")

df4_dateparts = DateParts(df3_9ma_rsi)
print(f"Date, Month and Year columns are added successfully, Num of rows processed: {df4_dateparts.count()}")

df5_Closes_90D_ago = Closes_90D_ago(df4_dateparts)
print(f"90 Days before close is calculated successfully, Num of rows processed: {df5_Closes_90D_ago.count()}")

df5_3Month_returns = Three_month_returns_Calculation(df4_dateparts, df5_Closes_90D_ago)
print(f"3 Month returns is calculated successfully, Num of rows processed: {df5_3Month_returns.count()}")

df6_previous_day_data = previous_day_data(df4_dateparts)
print(f"Previous day data is calculated successfully, Num of rows processed: {df6_previous_day_data.count()}")

# Loading Incrementally
increamental_load(Target_all_data_path, TargetFormat, df4_dateparts)
print("All data with all columns added rows loaded successfully")

increamental_load(Target_close_90D_ago, TargetFormat, df5_Closes_90D_ago)
print("closes before 90 days are loaded successfully")

increamental_load(Target_Final_data, TargetFormat, df5_3Month_returns)
print("3 month return calculated data is loaded successfully")

increamental_load(Target_previous_day_data, TargetFormat, df6_previous_day_data)
print("Previous day data is loaded successfully")

# Datacleanup: deleting year old rows from table
dataCleanup(DataRetentionPrd, Target_all_data_path, TargetFormat)
print("Removed rows from Target_all_data_path table which are older than 1 year")

dataCleanup(DataRetentionPrd, Target_close_90D_ago, TargetFormat)
print("Removed rows from Target_close_90D_ago table which are older than 1 year")

dataCleanup(DataRetentionPrd, Target_previous_day_data, TargetFormat)
print("Removed rows from Target_previous_day_data table which are older than 1 year")

dataCleanup(DataRetentionPrd, Target_Final_data, TargetFormat)
print("Removed rows from Target_Final_data table which are older than 1 year")
