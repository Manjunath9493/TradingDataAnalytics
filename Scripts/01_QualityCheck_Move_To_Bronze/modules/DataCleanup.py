def dataCleanup(DataRetentionPrd, LoadingDatapath, LoadingDataFormat):
    # Deleting older records from delta table (older than 365 days)
    try:
        from datetime import date, timedelta, datetime
        ToDate = date.today()
        FromDate_00 = ToDate - timedelta(days = DataRetentionPrd)
        FromDate = FromDate_00.strftime('%Y-%m-%d')
        
        from delta.tables import DeltaTable
        deltatbl = DeltaTable.forPath(spark, Datapath)
        deltatbl.delete(f"cast(run_date as date) < '{FromDate}'")
    
    except Exception as e:
        print(f"An error occured during cleanup: {e}")