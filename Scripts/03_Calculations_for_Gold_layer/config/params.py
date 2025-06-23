DataRetentionPrd = 365
SourceFormat = "Delta"
SourcePath = "abfss://trading-data@adlsstoragetradingdata.dfs.core.windows.net/silver/silver_layer_stockdata"
TargetFormat = "Delta"
Target_all_data_path = "abfss://trading-data@adlsstoragetradingdata.dfs.core.windows.net/gold/all_data/"
Target_close_90D_ago = "abfss://trading-data@adlsstoragetradingdata.dfs.core.windows.net/gold/close_90D_ago/"
Target_previous_day_data = "abfss://trading-data@adlsstoragetradingdata.dfs.core.windows.net/gold/previous_day_data/"
Target_Final_data = "abfss://trading-data@adlsstoragetradingdata.dfs.core.windows.net/gold/Final_data/"