def Folder_Cleanup(path):
    try:
        import IPython
        dbutils = IPython.get_ipython().user_ns["dbutils"]
    except:
        from pyspark.dbutils import DBUtils
        dbutils = DBUtils(spark)
            
    files = dbutils.fs.ls(path)
    for f in files:
        dbutils.fs.rm(f.path)