def configure_adls_oauth(spark):
    try:
        import IPython
        dbutils = IPython.get_ipython().user_ns["dbutils"]
    except:
        from pyspark.dbutils import DBUtils
        dbutils = DBUtils(spark)    
    
    spark.conf.set("fs.azure.account.auth.type.adlsstoragetradingdata.dfs.core.windows.net", "OAuth")
    spark.conf.set("fs.azure.account.oauth.provider.type.adlsstoragetradingdata.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
    spark.conf.set("fs.azure.account.oauth2.client.id.adlsstoragetradingdata.dfs.core.windows.net", "dd5b50b0-3e82-49da-bbd7-1c451e44b8bc")
    spark.conf.set("fs.azure.account.oauth2.client.secret.adlsstoragetradingdata.dfs.core.windows.net", dbutils.secrets.get(scope="kv-trading-data-analytic", key="ADLSAccessKeyForDatabricks-SP-OAUTH2"))
    spark.conf.set("fs.azure.account.oauth2.client.endpoint.adlsstoragetradingdata.dfs.core.windows.net", "https://login.microsoftonline.com/adb06f2f-16d7-437d-8aef-e88ac33564b6/oauth2/token")