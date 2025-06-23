from pyspark.sql import SparkSession
import pandas as pd
from datetime import datetime
from pyspark.sql.functions import *
from pyspark.sql.functions import col, lit
from pyspark.sql.types import StructType, StructField, DateType, IntegerType, StringType
from pyspark.sql.window import Window

from config.SparkSession import getSparkSession
from config.params import source_path, Logpath, Log_format, LogRetentionPrd, DataRetentionPrd, LoadingDatapath, LoadingDataFormat, Logmode, RawDataPath
from config.configure_adls import configure_adls_oauth

from modules.ReadCsv import readCsv
from modules.quality_check_logging import QualityCheckLogging
from modules.LogCleanup import logCleanup
from modules.Increamental_Loading import increamental_load
from modules.DataCleanup import dataCleanup
from modules.FolderCleanup import Folder_Cleanup

spark = getSparkSession("QualityCheck")
configure_adls_oauth(spark)

df = readCsv(source_path)
print(f"{df.count()} rows read successfully")

QualityCheckLogging(df, Logpath,Log_format,Logmode)
print("Quality check and Logging done successfully")

increamental_load(LoadingDatapath, LoadingDataFormat, df)
print("Loaded read data successfully")

logCleanup(LogRetentionPrd, Logpath, Log_format)
print("Removed logs older than 30 days")

dataCleanup(DataRetentionPrd, LoadingDatapath)
print("Removed data older than 365 days")
Folder_Cleanup(RawDataPath)
print("Removed files from Raw Data Path")



