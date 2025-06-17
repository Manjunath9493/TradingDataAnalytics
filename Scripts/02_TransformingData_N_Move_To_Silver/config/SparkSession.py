def getSparkSession(app_Name):
    return SparkSession.builder.appName(app_Name)\
    .config("spark.sql.session.timeZone", "Asia/Kolkata")\
    .getOrCreate()
   