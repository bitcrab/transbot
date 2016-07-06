import urllib.request
import urllib.error
import urllib.parse
import urllib
import json

BASE_URL = 'https://yunbi.com/'

API_BASE_PATH = '/api/v2'
API_PATH_DICT = {
    # GET
    'members': '%s/members/me.json',
    'markets': '%s/markets.json',

    #market code required in url as {market}.json
    'tickers' : '%s/tickers/%%s.json',
    #market required in url query string as '?market={market}'
    'orders': '%s/orders.json',

    #order id required in url query string as '?id={id}'
    'order': '%s/order.json',

    #market required in url query string as '?market={market}'
    'order_book': '%s/order_book.json',

    #market required in url query string as '?market={market}'
    'trades': '%s/trades.json',

    #market required in url query string as '?market={market}'
    'my_trades': '%s/trades/my.json',

    'k': '%s/k.json',
    #clear orders in all markets
    'clear': '%s/orders/clear.json',

    #delete a specific order
    'delete_order': '%s/order/delete.json',

    #TODO multi orders API
    'multi_orders': '%s/orders/multi.json',
}



def get_api_path(name):
    path_pattern = API_PATH_DICT[name]
    return path_pattern % API_BASE_PATH



class Client():

    def __init__(self, access_key=None, secret_key=None):
        if access_key and secret_key:
            self.auth = Auth(access_key, secret_key)
        else:
            pass
            #from conf import ACCESS_KEY, SECRET_KEY
            #self.auth = Auth(ACCESS_KEY, SECRET_KEY)

    def get(self, path, params=None, sigrequest=False):
        verb = "GET"
        if  sigrequest:
            signature, query = self.auth.sign_params(verb, path, params)
            query = self.auth.urlencode(query)
            url = "%s%s?%s&signature=%s" % (BASE_URL, path, query, signature)
        else:
            url = "%s%s?" % (BASE_URL, path)
        resp = urllib.request.urlopen(url)
        data = resp.readlines()
        if len(data):
            return json.loads(data[0].decode('utf-8'))


    def post(self, path, params=None):
        verb = "POST"
        print (params)
        signature, data = self.auth.sign_params(verb, path, params)
        url = "%s%s" % (BASE_URL, path)
        data.update({"signature":signature})
        data = urllib.parse.urlencode(data)
        data = data.encode('utf-8')
        resp = urllib.request.urlopen(url, data)
        data = resp.readlines()
        if len(data):
            return json.loads(data[0].decode('utf-8'))

#--------------------------------------------------------------------------------------

class Auth():
    def __init__(self, access_key, secret_key):
        self.access_key = access_key
        self.secret_key = secret_key

    def urlencode(self, params):
        keys = params.keys()
        keys = sorted(keys)
        query = ''
        for key in keys:
            value = params[key]
            if key != "orders":
               query = "%s&%s=%s" % (query, key, value) if len(query) else "%s=%s" % (key, value)
            else:
                #this ugly code is for multi orders API, there should be an elegant way to do this
                d = {key: params[key]}
                for v in value:
                    ks = v.keys()
                    ks = sorted(ks)
                    for k in ks:
                        item = "orders[][%s]=%s" % (k, v[k])
                        query = "%s&%s" % (query, item) if len(query) else "%s" % item
        return query

    def sign(self, verb, path, params=None):
        query = self.urlencode(params)
        msg = ("|".join([verb, path, query])).encode('utf-8')
        signature = hmac.new(self.secret_key.encode('utf-8'), msg=msg, digestmod=hashlib.sha256).hexdigest()

        return signature

    def sign_params(self, verb, path, params=None):
        if not params:
            params = {}
        params.update({'tonce': int(1000*time.time()), 'access_key': self.access_key})
        signature = self.sign(verb, path, params)

        return signature, params