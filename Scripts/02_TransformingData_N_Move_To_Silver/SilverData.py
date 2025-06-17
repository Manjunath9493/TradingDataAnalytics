from pyspark.sql import SparkSession
import pandas as pd
from datetime import datetime
from pyspark.sql.functions import *
from pyspark.sql.functions import col, lit
from pyspark.sql.types import StructType, StructField, DateType, IntegerType, StringType
from pyspark.sql.window import Window
from pyspark.sql.functions import regexp_replace

from config.SparkSession import getSparkSession
from cofig.params import DataRetentionPrd, sourcePath, dataFormat, silver_data_path

from modules.Read import read
from modules.transform_data import Transform_Data
from modules.Increamental_Loading import increamental_load
from modules.DataCleanup import dataCleanup

from lib.install_packages import install_packages
install_packages()

spark = getSparkSession("Transform_Data")
df = read(dataFormat,sourcePath)
transformed_df = Transform_Data(df)

LoadingPath = silver_data_path
LoadFormat = dataFormat
loadingDF = transformed_df
increamental_load(LoadingPath, LoadFormat, loadingDF)

dataCleanup(DataRetentionPrd, LoadingPath, LoadFormat)