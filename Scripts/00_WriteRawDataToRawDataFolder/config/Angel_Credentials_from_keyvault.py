api_key = dbutils.secrets.get(scope = "AngelOne-API-Key", key = "AngelOne-API-Key" )
client_code = dbutils.secrets.get(scope = "AngelOne-client-code", key = "AngelOne-client-code")
password =dbutils.secrets.get(scope = "AngelOne-password",key = "AngelOne-password")
totp_key = dbutils.secrets.get(scope = "AngelOne-Totp-key", key = "AngelOne-Totp-key")
