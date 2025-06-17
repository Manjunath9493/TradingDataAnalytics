from pyspark.sql import SparkSession
from SmartApi import SmartConnect
import pyotp
import pandas as pd
from datetime import datetime
from pyspark.sql.functions import *
from pyspark.sql.functions import col, lit
from pyspark.sql.types import StructType, StructField, DateType, IntegerType, StringType
from pyspark.sql.window import Window

from config.Angel_Credentials_from_keyvault import api_key, client_code, password, totp_key
from config.AngelOne_Connection import Create_AngelOne_Connection
from config.SparkSession import getSparkSession
from config.params import raw_data_path, FromDate_formatted, ToDate_formatted, interval, SymToken_Symbol_pairs

from modules.StockDataExtractor import ForLoopForStockData
from modules.write import WriteRawInputCSVFiles
from modules.extract import GetHistoricalOHLCData
from modules.EmptyDfForStockData import EmptyDfForStockData

from lib.install_packages import install_packages
install_packages()

data = Create_AngelOne_Connection(api_key, client_code, password, totp_key)
spark = getSparkSession("RawData")
df = ForLoopForStockData(SymToken_Symbol_pairs, interval, FromDate_formatted, ToDate_formatted)

WriteRawInputCSVFiles(raw_data_path)