from config.SparkSession import getSparkSession
spark = getSparkSession("QualityCheck")
def logCleanup(LogRetentionPrd, Logpath, Log_format):
    try:
        from datetime import date, timedelta, datetime
        ToDate = date.today()
        FromDate_00 = ToDate - timedelta(days = LogRetentionPrd)
        FromDate = FromDate_00.strftime('%Y-%m-%d')
        
        from delta.tables import DeltaTable
        deltatbl = DeltaTable.forPath(spark, Logpath)
        deltatbl.delete(f"cast(run_date as date) < '{FromDate}'")
    
    except Exception as e:
        print(f"An error occured during cleanup: {e}")
        
    try:
        df = spark.read.format(Log_format).load(Logpath)
        dedup_df = df.dropDuplicates()
        Write_df_to_ADLS(dedup_df,"Overwrite", Log_format, Logpath)
        
    except Exception as e:
        print(f"An Error occured during deduplication: {e}")
