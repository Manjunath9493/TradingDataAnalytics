from datetime import date, timedelta, datetime
from delta.tables import DeltaTable

def dataCleanup(DataRetentionPrd, LoadingDatapath):
    # Deleting older records from delta table (older than 365 days)
    try:
        ToDate = date.today()
        FromDate_00 = ToDate - timedelta(days = DataRetentionPrd)
        FromDate = FromDate_00.strftime('%Y-%m-%d')
        
        deltatbl = DeltaTable.forPath(spark, LoadingDatapath)
        deltatbl.delete(f"cast(run_date as date) < '{FromDate}'")
    
    except Exception as e:
        print(f"An error occured during cleanup: {e}")