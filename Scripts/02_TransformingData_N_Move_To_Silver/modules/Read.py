def read(dataFormat,SourcePath):
    reader = spark.read.format(dataFormat)
    if dataFormat.lower() == "csv":
        reader = reader.option("header", True).option("inferschema","true")
	return reader.load(SourcePath)