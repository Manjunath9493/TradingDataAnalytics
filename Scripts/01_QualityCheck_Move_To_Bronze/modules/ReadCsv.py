from config.SparkSession import getSparkSession
spark = getSparkSession("QualityCheck")

def readCsv(SourcePath):
	return spark.read.format("csv").option("header", True).option("inferschema","true").load(SourcePath)