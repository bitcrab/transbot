import urllib.request
import urllib.error
import urllib.parse
import urllib
import json
from lib.auth import Auth


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

