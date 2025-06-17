def EmptyDfForStockData():
	schema = StructType([
		StructField("Date", DateType(), True),
		StructField("Open", IntegerType(), True),
		StructField("High", IntegerType(), True),
		StructField("Low", IntegerType(), True),
		StructField("Close", IntegerType(), True),
		StructField("Volume", IntegerType(), True),
		StructField("Symbol", StringType(), True)
	])
	return spark.createDataFrame([], schema)