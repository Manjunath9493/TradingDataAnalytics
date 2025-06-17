### 00_WriteRawDataToRawDataFolder/RawData.py
Ingests historical OHLC stock data from the AngelOne SmartAPI using secure credentials stored in Azure KeyVault. 
Converts data into Spark DataFrames and writes it to the Raw Layer in ADLS in CSV format.

- Reads token-symbol pairs and config from `params.py`
- Connects via TOTP-authenticated API
- Fetches 1 year data for each stock and stores data Rawdata container in ADLS


### 01_QualityCheck_Move_To_Bronze/BronzeData.py
Performs quality checks on raw data and moves it to the Bronze Layer.

- Quality Checks like number of nulls in each column, duplicates, zero/-ve valued stock data, invalid rows like high < low, open>high etc
- Logs this info into a delta table
- Incrementally loaded the read data from rawinput folder to bronze layer in ADLS in delta format
- Implemented cleanup activity for the data older than the retention period for both logs and bronze layer data (30 days for Logs and 365 days for bronze layer Data)
- Implemented Folder cleanup activity for RawData container at the end of this step as the same data is copied to bronze layer.

---

### 02_TransformingData_N_Move_To_Silver/SilverData.py
Transforms Bronze data by renaming and standardizing columns

- read data from bronze layer 
- removed unnecessary string from stock name
- Saves in delta format for efficient querying in Silverlayer container in ADLS
- Incrementally loaded the transformed data to silverlayer in ADLS in delta format
- Implemented cleanup activity for the data older than the retention period for silverlayer data (365 days)

---

### 03_Calculations_for_Gold_layer/GoldData.py
Performs final calculations and feature engineering for downstream analytics or modeling.

- Calculated moving averages, RSI, 9Ma on RSI and 3-month returns
- Adds time-based partitions using `Dateparts.py`
- Incrementally loaded this calculated data to Goldlayer in ADLS in delta format
- Implemented cleanup activity for the data older than the retention period for Goldlayer data (365 days) for all
