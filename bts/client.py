class Config():
    pass

class Client()：
    def __init__(self, access_key=None, secret_key=None, account_id=None):



    if access_key and secret_key:
        self.access_key = access_key
        self.secret_key = secret_key
        self.mdt = "%s_%s_%s" % (access_key, account_id, secret_key)
    else:
        pass
        # from conf import ACCESS_KEY, SECRET_KEY