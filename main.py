import json
import yunbi.client
import yunbi
import btc38
import btc38.client
import pymysql.cursors
from grapheneexchange import GrapheneExchange
import asyncio
from grapheneapi.grapheneclient import GrapheneClient
import pprint
import time
from datetime import datetime


class MarketMaker(object):
    def __init__(self):

        f = open("config.json", 'r')
        config = json.loads(f.read())
        f.close()

        for client in config:

            if client['client'] == 'bts':
                class Config():
                    pass

                btsConfig = Config()
                btsConfig.witness_url = client['WITNESS_URL']
                btsConfig.witnes_user = ""
                btsConfig.witness_password = ""
                btsConfig.watch_markets = ["CNY_BTS", "BTS_CNY"]
                btsConfig.market_separator = "_"
                btsConfig.account = client['ACCOUNT']
                btsConfig.wif = client['SECRET_KEY']

                self.btsClient = GrapheneExchange(btsConfig, safe_mode=False)

            if client['client'] == 'yunbi':
                self.yunbiClient = yunbi.client.Client(client['ACCESS_KEY'], client['SECRET_KEY'])

            if client['client'] == 'btc38':
                self.btc38Client = btc38.client.Client(client['ACCESS_KEY'], client['SECRET_KEY'], client['ACCOUNT_ID'])

            if client['client'] == 'mysql':
                self.mysqlClient = pymysql.connect(host=client['host'], user=client['user'], password=client['password'],
                                              database=client['database'])
        self.currentDEXMiddlePrice = 0

    def dexTicker2General(self,dexTicker):
        ticker = {"vol": dexTicker["quoteVolume"], "buy": dexTicker["highestBid"], "last": dexTicker["last"],
                  "sell": dexTicker["lowestAsk"]}
        return ticker

    def executeOrder(self,exchange,Order):
        if exchange == "btc38":
            if Order["type"] == "buy":
                type = 1
            if Order["type"] == "sell":
                type = 2

            return self.btc38Client.submitOrder(type, 'cny', Order["price"], Order["volume"], "bts")
        if exchange == 'dex':
            if Order["type"] == "buy":
                return json.dumps(self.btsClient.buy("BTS_CNY", Order["price"], Order["volume"]))
            if Order["type"] == "sell":
                return json.dumps(self.btsClient.sell("BTS_CNY", Order["price"], Order["volume"]))

    def cancelAllOrders(self,exchanges=['dex','btc38'],quote = "bts"):
        for ex in exchanges:
            if ex == "dex":
                orders = self.btsClient.returnOpenOrders("BTS_CNY")['BTS_CNY']
                for order in orders:
                    print ("DEX order canceled:" )
                    print(self.btsClient.cancel(order["orderNumber"]))
            if ex == "btc38":
                orders = self.btc38Client.getOrderList("bts")
                for order in orders:
                    print("btc38 order canceled")
                    print(self.btc38Client.cancelOrder("cny",order["id"]))
        return

    def clearTicker(self, exchanges = ['dex','btc38']):
        btc38Ticker = self.btc38Client.getTickers()['ticker']
        dexTicker =  self.dexTicker2General(self.btsClient.returnTicker()['BTS_CNY'])

        minGap = (btc38Ticker['buy'] + btc38Ticker['sell']) *0.0015

        btc38OrderBook = self.btc38Client.getDepth('bts')
        dexOrderBook = self.btsClient.returnOrderBook("BTS_CNY")['BTS_CNY']

        highex = None

        if (btc38Ticker['buy']-minGap) > dexTicker['sell']:
            highex = 'btc38'
            highTicker = btc38Ticker
            highOrderBook = btc38OrderBook
            lowex = 'dex'
            lowTicker = dexTicker
            lowOrderBook = dexOrderBook

        if (btc38Ticker['sell']+minGap) < dexTicker['buy']:
            lowex = 'btc38'
            lowTicker = btc38Ticker
            lowOrderBook = btc38OrderBook
            highex = 'dex'
            highTicker = dexTicker
            highOrderBook = dexOrderBook

        if not highex:
            #btc38MiddlePrice = (btc38Ticker["buy"] + btc38Ticker["sell"]) / 2
            #if abs(btc38MiddlePrice - self.currentDEXMiddlePrice) > minGap*1.5:
            return 0
        else:
            self.cancelAllOrders()
            print("have removed the orders with potential to be arbitraged!")
            time.sleep(4)
            btc38Ticker = self.btc38Client.getTickers()['ticker']
            dexTicker = self.dexTicker2General(self.btsClient.returnTicker()['BTS_CNY'])
            btc38OrderBook = self.btc38Client.getDepth('bts')
            dexOrderBook = self.btsClient.returnOrderBook("BTS_CNY")['BTS_CNY']
            highex = None
            if (btc38Ticker['buy'] - minGap) > dexTicker['sell']:
                highex = 'btc38'
                highTicker = btc38Ticker
                highOrderBook = btc38OrderBook
                lowex = 'dex'
                lowTicker = dexTicker
                lowOrderBook = dexOrderBook

            if (btc38Ticker['sell'] + minGap) < dexTicker['buy']:
                lowex = 'btc38'
                lowTicker = btc38Ticker
                lowOrderBook = btc38OrderBook
                highex = 'dex'
                highTicker = dexTicker
                highOrderBook = dexOrderBook

            if not highex:

                return 1

        BidOrder = {"type":"buy", "volume":lowOrderBook["asks"][0][1], "price":lowTicker['sell'],"index":0}
        AskOrder = {"type":"sell","volume":highOrderBook["bids"][0][1], "price":highTicker['buy'],"index":0}

        while highOrderBook["bids"][AskOrder['index']][0] > (lowOrderBook["asks"][BidOrder["index"]][0]+minGap): #BidOrder["price"] < (AskOrder["price"] - minGap):
            pointBidOrder = BidOrder["volume"] > AskOrder["volume"]
            if pointBidOrder:
                AskOrder["index"] += 1
                if highOrderBook["bids"][AskOrder['index']][0] > (BidOrder["price"] + minGap):
                    AskOrder["volume"] += highOrderBook["bids"][AskOrder['index']][1]
                    AskOrder["price"] = highOrderBook["bids"][AskOrder['index']][0]
            else:
                BidOrder["index"] += 1
                if lowOrderBook["asks"][BidOrder['index']][0] < (AskOrder["price"] - minGap):
                    BidOrder["volume"] += lowOrderBook["asks"][BidOrder["index"]][1]
                    BidOrder["price"] = lowOrderBook["asks"][BidOrder["index"]][0]
        BidOrder["volume"] = min(BidOrder["volume"], AskOrder["volume"])
        AskOrder["volume"] = BidOrder["volume"]

        print(self.executeOrder(lowex,BidOrder))
        print(self.executeOrder(highex,AskOrder))
        print("have tried to sumit orders for arbitrage!")
        print(AskOrder)
        print(BidOrder)
        return 1


    def generateMakerOrder(self,exchanges = ['dex','btc38'], volume = 40000):
        btc38Ticker = self.btc38Client.getTickers()['ticker']
        dexTicker = self.dexTicker2General(self.btsClient.returnTicker()['BTS_CNY'])
        middlePrice = (btc38Ticker["buy"] +btc38Ticker["sell"])/2
        settlePrice = self.btsClient.returnTicker()['BTS_CNY']['settlement_price']
        bidPrice = max((btc38Ticker["buy"]+0.00009),middlePrice*0.995)
        askPrice = max(settlePrice*1.01,middlePrice*1.015)
        BidOrder = [{"type": "buy", "volume": volume, "price": bidPrice},{"type": "buy", "volume": volume, "price": bidPrice*0.99}]
        AskOrder = [{"type": "sell", "volume": volume*0.3, "price": askPrice},{"type": "sell", "volume": volume, "price": askPrice*1.01}]
        for n in [0,1]:
            print("try to create dex bid order: %s" % BidOrder[n])
            print(BidOrder[n])
            print(self.executeOrder("dex",BidOrder[n]))
            print("try to create dex ask order:")
            print(AskOrder[n])
            print(self.executeOrder("dex",AskOrder[n]))
            print(AskOrder[n])
        self.currentDEXMiddlePrice = middlePrice
        return middlePrice

    #@asyncio.coroutine
    def run(self, loopnumber=20):
        try:
            while True:
                n = 0
                for n in list(range(loopnumber)):
                    if (self.clearTicker()):
                        self.generateMakerOrder()
                        n = 0
                    else:
                        print("now there is no chance for arbitrage, n=%s in %s circles  %s" % (n,loopnumber, datetime.now()))
                    time.sleep(20)
                    n += 1
                self.cancelAllOrders(["dex"])
                self.generateMakerOrder()
        except :
            print("some error happened")

maker = MarketMaker()
loop = asyncio.get_event_loop()
#maker = MarketMaker()
#maker.run()
#task = [loop.create_task(maker.run()]
loop.run_until_complete(asyncio.wait(maker.run()))
loop.run_forever()

"""
while True:
    print(maker.clearTicker())
    print(datetime.now())
    time.sleep(50)


#Askorder = {'volume': 10, 'index': 4, 'type': 'buy', 'price': 0.027248999973145567}
#maker.executeOrder('btc38',Askorder)

print(btsClient.cancel_asks_less_than("BTS_CNY",50))
print(btsClient.cancel_bids_more_than("BTS_CNY",0.027))
print(btsClient.cancel_asks_less_than("CNY_BTS",50))
print(btsClient.cancel_bids_more_than("CNY_BTS",0.027))
print(btsClient.get_my_asks_less_than("BTS_CNY",50))
print(btsClient.get_my_asks_less_than("CNY_BTS",50))
print(btsClient.markets)
if __name__ == '__main__':
    pass
    #dex   = btsClient
    #print(json.dumps(dex.returnTradeHistory("CNY_BTS"),indent=4))
    #print(json.dumps(dex.returnTicker()['BTS_CNY'],indent=4))
    #print(dexTicker2General(dex.returnTicker()['BTS_CNY']))
    #print(json.dumps(dex.return24Volume(),indent=4))
    #print(json.dumps(btsClient.returnOrderBook("BTS_CNY"),indent=4))
    #print(json.dumps(dex.returnBalances(),indent=4))
    #print(json.dumps(dex.returnOpenOrders("all"),indent=4))
    #print(json.dumps(dex.buy("CNY_BTS", 0.001, 10),indent=4))
#print(json.dumps(btsClient.sell("CNY_BTS", 1/0.028, 1),indent=4))
#print(json.dumps(btsClient.sell("BTS_CNY", 0.028, 10),indent=4))
    #a="dex.returnTicker()['BTS_CNY']"
    #eval(a)
    #print(json.dumps(btc38Client.getDepth('bts'),indent=4))
    #print(json.dumps(btc38Client.getTickers('bts'), ))#indent=4))





class Config1():
    witness_url           = "wss://bitshares.dacplay.org:8089/ws"

if __name__ == '__main__':
    client = GrapheneClient(Config1)
    for b in client.ws.stream("transfer"):
        pprint(b)


for n in list(range(30)):

    trades = btc38Client.getMyTradeList(page=n+2)
    try:
        with mysqlClient.cursor() as cursor:
            for record in trades:
                print(record)
                paramstr = "('%s', '%s', '%s', '%s', '%s', '%f', '%f', '%s')" % (
                record['id'], 'btc38', record['buyer_id'], record['seller_id'], record['coinname'],
                float(record['price']), float(record['volume']), record['time'])
                sql = "INSERT INTO `botdb` (`id`,`exchange`,`buyer_id`,`seller_id`,`asset`,`price`,`volume`,`time`) VALUES " + paramstr
                print(sql)

                cursor.execute(sql)
            mysqlClient.commit()

    finally:
        # mysqlClient.close()
        pass



print(btsClient)
balance = btsClient.returnBalances()
print(balance)

this is for product



"""

"""
    else:
        self.cancelAllOrders(exchanges,"bts")
        highex = None
        if (btc38Ticker['buy'] - minGap) > dexTicker['sell']:
            highex = 'btc38'
            highTicker = btc38Ticker
            highOrderBook = btc38OrderBook
            lowex = 'dex'
            lowTicker = dexTicker
            lowOrderBook = dexOrderBook

        if (btc38Ticker['sell'] + minGap) < dexTicker['buy']:
            lowex = 'btc38'
            lowTicker = btc38Ticker
            lowOrderBook = btc38OrderBook
            highex = 'dex'
            highTicker = dexTicker
            highOrderBook = dexOrderBook
        if not highex:
            print("now there is no chance for arbitrary trade among the exchages")
            return ("now there is no chance for arbitrary trade among the exchages")





EX_DICT = {
    "btc38Ticker"     : "btc38Client.getTickers()['ticker']",
    "btc38OrderBook"  : "btc38Client.getDepth('bts')",
    "dexTicker"       : "dex.returnTicker()['BTS_CNY']",
    "dexOrderBook"    : "dex.returnOrderBook('BTS_CNY')"


}





"""