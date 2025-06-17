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
from config.params import source_path, Logpath, Log_format, LogRetentionPrd, DataRetentionPrd, LoadingDatapath, LoadingDataFormat, Logmode, RawDataPath

from modules.ReadCsv import readCsv
from modules.quality_check_logging import QualityCheckLogging
from modules.LogCleanup import logCleanup
from modules.Increamental_Loading import increamental_load
from modules.DataCleanup import dataCleanup
from modules.FolderCleanup import Folder_Cleanup

from lib.install_packages import install_packages
install_packages()

spark = getSparkSession("QualityCheck")
df = readCsv(source_path)

QualityCheckLogging(df, Logpath,Log_format,Logmode)
increamental_load(LoadingDatapath, LoadingDataFormat, df)

logCleanup(LogRetentionPrd, Logpath, Log_format)
dataCleanup(DataRetentionPrd, LoadingDatapath, LoadingDataFormat)
Folder_Cleanup(RawDataPath)