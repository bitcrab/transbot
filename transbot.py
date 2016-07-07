from bts.http_rpc import HTTPRPC
import asyncio
import time, sys, queue, math, json
from pprint import pprint
from grapheneexchange import GrapheneExchange
from multiprocessing.managers import BaseManager

class QueueManager(BaseManager):
    pass

class Config():
    witness_url           = "wss://bitshares.dacplay.org:8089/ws"
    witness_user          = ""
    witness_password      = ""

    watch_markets         = ["CNY_BTS","BTS_CNY"]
    market_separator      = "_"
    account               = "transwiser.test"
    wif                   = "5KfSRygyqDfUyeUfSbFt8qM7XNA4iGb35QogRJAYza6Z9erPiK8"

class Transbots(object):
    def __init__(self,config):
        self.dex = GrapheneExchange(config, safe_mode=False)
        GrapheneExchange()

        QueueManager.register('get_task_queue')
        QueueManager.register('get_result_queue')

        server_addr = '127.0.0.1'
        print('Connect to server %s...' % server_addr)
        # 端口和验证码注意保持与task_master.py设置的完全一致:
        m = QueueManager(address=(server_addr, 5000), authkey=b'abc')
        # 从网络连接:
        m.connect()
        # 获取Queue的对象:
        self.btspricetask = m.get_task_queue()
        self.btspriceresult = m.get_result_queue()
        self.btsbuyfactor = 1




    def GetBtsPricefromQueue(self):
        _bts_market_price = None
        while not _bts_market_price:
                        while not self.btspricetask.empty():
                            _bts_market_price = self.btspricetask.get()
                            print('bts marketprice:', '\n', _bts_market_price)
                        time.sleep(5)
                        print('wait for 5 seconds')
        return _bts_market_price

    @asyncio.coroutine
    def task_bots(self):
        while True:
            try:
                self.Check_Order()
            except Exception as e:
                print("task bots error:", e)
            #self.display_order()
            yield from asyncio.sleep(10)


    def Check_Order(self):
        openorders = self.dex.returnOpenOrders("BTS_CNY")


        #pprint(openorders['BTS_CNY'])
        marketprice = self.GetBtsPricefromQueue()
        middleprice =(marketprice["buy"]+marketprice["sell"])/2

        if middleprice < 3.5:
            self.btsbuyfactor = 1.2


        orderSum = [0,0]
        for order in openorders["BTS_CNY"]:
             if order['type'] == 'buy':
                 if (order['rate'] > middleprice*1.01 or order['rate'] < marketprice["buy"]*0.97):
                     print('cancel buy order', order['orderNumber'])
                     pprint(self.dex.cancel(order['orderNumber']))
                 else:
                     orderSum[0] += order['amount']
             if order['type'] == 'sell':
                 if (order['rate'] < middleprice * 0.995 or order['rate'] > marketprice["sell"] * 1.03):
                     print('cancel sell order', order['orderNumber'])
                     pprint(self.dex.cancel(order['orderNumber']))
                 else:
                     orderSum[1] += order['amount']
        print('total buy order amount:', orderSum[0], 'BTS')
        print('total sell order amount:', orderSum[1], 'BTS')

        balance = self.dex.returnBalances()
        pprint(balance)

        needBTS = (balance['BTS'] + orderSum[1] + orderSum[0] + balance['CNY']/middleprice)*self.btsbuyfactor/2 - balance['BTS'] - orderSum[1]
        needCNY = (balance['CNY'] + (balance['BTS'] + orderSum[1] + orderSum[0]) * middleprice)*(1-self.btsbuyfactor/2) - balance['CNY'] - orderSum[0]*middleprice

        #pprint(self.dex.buy('CNY_BTS', 1 / middleprice, 1))

        if needBTS > 5000:
            pprint(self.dex.sell('CNY_BTS', 1/(middleprice*0.997), needBTS*middleprice*0.95))

        if needCNY > 100:
            pprint(self.dex.buy('CNY_BTS', 1/(middleprice*1.01), needCNY))

        #if orderSum[0] < bal


    def run(self):
        loop = asyncio.get_event_loop()
        loop.create_task(self.task_bots())
        loop.run_forever()
        loop.close()

trans_bot = Transbots(Config)
trans_bot.run()



