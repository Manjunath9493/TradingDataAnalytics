from pyspark.sql import SparkSession
from config.SparkSession import getSparkSession
spark = getSparkSession("Calculations")

def read(dataFormat,SourcePath):
    reader = spark.read.format(dataFormat)
    if dataFormat.lower() == "csv":
        reader = reader.option("header", True).option("inferschema","true")
    return reader.load(SourcePath)