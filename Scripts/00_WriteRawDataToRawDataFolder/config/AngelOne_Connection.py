def Create_AngelOne_Connection(api_key, client_code, password, totp_key):
    totp = pyotp.TOTP(totp_key).now()
    obj = SmartConnect(api_key=api_key)
    return  obj.generateSession(client_code, password, totp)

