import urllib.request
import urllib.error
import urllib.parse
import urllib
import json
import time
import hashlib

BASE_URL = 'http://api.btc38.com/v1/'

API_PATH_DICT = {
    # GET

    #market code required in url as {market}.json
    'tickers' : 'ticker.php?',
    #'tickers' : 'ticker.php?c=%s&mk_type=%s',

    'depth'   :'depth.php?',
    #'depth'   :'depth.php?c=%s&mk_type=%s',

    #order id required in url query string as '?id={id}'
    'orders': 'getOrderList.php',

    #market required in url query string as '?market={market}'
    'trades': 'trades.php?',
    #'trades': 'trades.php?c=%s&mk_type=%s&tid=%s',

    #POST

    'balance': 'getMyBalance.php',

    'submitorder':'submitOrder.php',

    'cancelorder': 'cancelOrder.php',

    'myorders': 'getOrderList.php',

    #market required in url query string as '?market={market}'
    'mytrades': 'getMyTradeList.php',


}



def get_api_path(name):
    path_pattern = API_PATH_DICT[name]
    return  BASE_URL + path_pattern



class Client():

    def __init__(self, access_key=None, secret_key=None, account_id=None):
        if access_key and secret_key:
            self.access_key = access_key
            self.secret_key = secret_key
            self.mdt = "%s_%s_%s" % (access_key,account_id,secret_key)
        else:
            print("please provide correct keys")


    def request(self, name, data=None, c=None, mk_type = None, tid=None ):

        headers = {'User-Agent': 'Mozilla/4.0'}
        url = get_api_path(name)

        if c:
            query = "c=%s&mk_type=%s" % (c, mk_type)
            if tid:
                query += "&tid=%s" % tid
            url += query


        if data:
            data = urllib.parse.urlencode(data)
            data = data.encode('utf-8')

        req = urllib.request.Request(url=url, data=None, headers=headers) if not data else urllib.request.Request(url=url, data=data, headers=headers)
        resp = urllib.request.urlopen(req,timeout=5)
        result = resp.readlines()
        resp.close()
        return result

    def getTickers(self,mk_type='cny',c='bts'):
        result = self.request('tickers',c=c, mk_type = mk_type)
        return json.loads(result[0].decode('utf-8'))

    def getDepth(self, mk_type='cny', c='bts'):
        result = self.request('depth', c=c, mk_type=mk_type)
        return json.loads(result[0].decode('utf-8'))

    def getMyBalance(self):
        timestamp, MD5 = self.getMD5()
        params = {'key': self.access_key, 'time': timestamp, 'md5': MD5}
        result =  self.request("balance",params)
        return json.loads(result[0].decode('utf-8'))

    def submitOrder(self, type, mk_type, price, amount, coinname):#type: 1 for buy, and 2 for sell
        timestamp, MD5 = self.getMD5()
        params = {'key': self.access_key, 'time': timestamp, 'md5': MD5, 'type':type, 'mk_type':mk_type, 'price':price, 'amount':amount, 'coinname':coinname}
        return  self.request("submitorder", params)


    def cancelOrder(self,mk_type,order_id):
        timestamp, MD5 = self.getMD5()
        params = {'key': self.access_key, 'time': timestamp, 'md5': MD5, 'mk_type': mk_type, 'order_id': order_id}
        return self.request("cancelorder", params)

    def getOrderList(self,coinname = None):
        timestamp, MD5 = self.getMD5()
        params = {'key': self.access_key, 'time': timestamp, 'md5': MD5, 'coinname':coinname}
        result =  self.request("myorders", params)
        if result == [b'no_order']:
            return []
        return json.loads(result[0].decode('utf-8'))


    def getMyTradeList(self,mk_type='cny',coinname='bts',page=1):
        timestamp, MD5 = self.getMD5()
        params = {'key': self.access_key, 'time': timestamp, 'md5': MD5, 'mk_type': mk_type, 'coinname': coinname, 'page':page}
        result = self.request("mytrades", params)
        return json.loads(result[0].decode('utf-8'))

    def getMD5(self):
        stamp = int(time.time())
        mdt = "%s_%s" % (self.mdt, stamp)
        md5 = hashlib.md5()
        md5.update(mdt.encode('utf-8'))
        return stamp, md5.hexdigest()


