from config.SparkSession import getSparkSession
spark = getSparkSession("QualityCheck")
from delta.tables import DeltaTable
def increamental_load(LoadingPath, LoadFormat, loadingDF):     
    try:
        import IPython
        dbutils = IPython.get_ipython().user_ns["dbutils"]
    except:
        from pyspark.dbutils import DBUtils
        dbutils = DBUtils(spark)
            
    try:
        dbutils.fs.ls(LoadingPath)
        path_exists = True
    except:
        path_exists = False
    
    
    if not path_exists:
        loadingDF.write.mode("overwrite").format(LoadFormat).save(LoadingPath)
    else:
        # Loading the Delta table
        target = DeltaTable.forPath(spark, LoadingPath)
        
        # Merge condition
        merge_condition = "target.Date = source.Date and target.symbol = source.symbol"
        
        # Incremental Loading with Merge statement
        target.alias("target").merge(
            source = loadingDF.alias("source"),
            condition = merge_condition
        ).whenMatchedUpdateAll() \
        .whenNotMatchedInsertAll() \
        .execute()