import json
import yunbi.client
import yunbi
import btc38
import btc38.client
import pymysql.cursors
from grapheneexchange import GrapheneExchange
import asyncio
import time
from datetime import datetime, timedelta
import hashlib


class TradeClient(object):
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
                self.mysqlClient = pymysql.connect(host=client['host'], user=client['user'],
                                                   password=client['password'],
                                                   database=client['database'])


class MarketMaker(object):
    def __init__(self):
        self.client = TradeClient()
        self.currentDEXMiddlePrice = 0
        self.makingvolume = 50000

    def dexTicker2General(self, dexTicker):
        ticker = {"vol": dexTicker["quoteVolume"], "buy": dexTicker["highestBid"], "last": dexTicker["last"],
                  "sell": dexTicker["lowestAsk"]}
        return ticker

    def executeOrder(self, exchange, Order):
        if exchange == "btc38":
            if Order["type"] == "buy":
                type = 1
            if Order["type"] == "sell":
                type = 2

            return self.client.btc38Client.submitOrder(type, 'cny', Order["price"], Order["volume"], "bts")
        if exchange == 'dex':
            if Order["type"] == "buy":
                return json.dumps(self.client.btsClient.buy("BTS_CNY", Order["price"], Order["volume"]))
            if Order["type"] == "sell":
                return json.dumps(self.client.btsClient.sell("BTS_CNY", Order["price"], Order["volume"]))

    def cancelAllOrders(self, exchanges=['dex', 'btc38'], quote="bts"):
        for ex in exchanges:
            if ex == "dex":
                orders = self.client.btsClient.returnOpenOrders("BTS_CNY")['BTS_CNY']
                for order in orders:
                    print("DEX order canceled:")
                    print(self.client.btsClient.cancel(order["orderNumber"]))
            if ex == "btc38":
                orders = self.client.btc38Client.getOrderList("bts")
                for order in orders:
                    print("btc38 order canceled")
                    print(self.client.btc38Client.cancelOrder("cny", order["id"]))
        return

    def clearTicker(self, exchanges=['dex', 'btc38']):
        btc38Ticker = self.client.btc38Client.getTickers()['ticker']
        dexTicker = self.dexTicker2General(self.client.btsClient.returnTicker()['BTS_CNY'])

        middlePrice = (btc38Ticker['buy'] + btc38Ticker['sell'])/2
        minGap = middlePrice*0.003

        btc38OrderBook = self.client.btc38Client.getDepth('bts')
        dexOrderBook = self.client.btsClient.returnOrderBook("BTS_CNY")['BTS_CNY']

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

        if not highex: # no arbitrage chance, then check whether need to regernate oders
            dexopenorders = self.client.btsClient.returnOpenOrders("BTS_CNY")["BTS_CNY"]
            sumOpenOrderAmount = 0
            for order in dexopenorders:
                sumOpenOrderAmount += order["amount"]
            priceshift = middlePrice - self.currentDEXMiddlePrice
            if (abs(priceshift) >minGap*2) or (sumOpenOrderAmount<self.makingvolume*2.31):
                #if the market price shifted too much or some orders is filled enough, then regernate orders
                self.cancelAllOrders()
                print("deleted orders for regeneration as price shifted too much or too much order filled, middle price = %s, price shift = %s, minGap = %s, left order volums = %s BTS." % (middlePrice, priceshift, minGap, sumOpenOrderAmount))
                return 1
            else:
                return 0
        else:#arbitrage chance, check more
            self.cancelAllOrders()
            print("have removed the orders with potential to be arbitraged!")
            time.sleep(4)
            btc38Ticker = self.client.btc38Client.getTickers()['ticker']
            dexTicker = self.dexTicker2General(self.client.btsClient.returnTicker()['BTS_CNY'])
            btc38OrderBook = self.client.btc38Client.getDepth('bts')
            dexOrderBook = self.client.btsClient.returnOrderBook("BTS_CNY")['BTS_CNY']
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

            if not highex: #no arbitrage after removing own orders
                return 1

        BidOrder = {"type": "buy", "volume": lowOrderBook["asks"][0][1], "price": lowTicker['sell'], "index": 0}
        AskOrder = {"type": "sell", "volume": highOrderBook["bids"][0][1], "price": highTicker['buy'], "index": 0}

        while highOrderBook["bids"][AskOrder['index']][0] > (
            lowOrderBook["asks"][BidOrder["index"]][0] + minGap):  # BidOrder["price"] < (AskOrder["price"] - minGap):
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

        print(self.executeOrder(lowex, BidOrder))
        print(self.executeOrder(highex, AskOrder))
        print("have tried to sumit orders for arbitrage!")
        print(AskOrder)
        print(BidOrder)
        return 1

    def generateMakerOrder(self, exchanges=['dex', 'btc38']):
        btc38Ticker = self.client.btc38Client.getTickers()['ticker']
        dexTicker = self.dexTicker2General(self.client.btsClient.returnTicker()['BTS_CNY'])
        middlePrice = (btc38Ticker["buy"] + btc38Ticker["sell"]) / 2
        settlePrice = self.client.btsClient.returnTicker()['BTS_CNY']['settlement_price']
        bidPrice = max((btc38Ticker["buy"] ), middlePrice * 0.995)
        askPrice = max(settlePrice * 1.01, middlePrice * 1.012)
        BidOrder = [{"type": "buy", "volume": self.makingvolume, "price": bidPrice},
                    {"type": "buy", "volume": self.makingvolume, "price": bidPrice * 0.99}]
        AskOrder = [{"type": "sell", "volume": self.makingvolume * 0.3, "price": askPrice},
                    {"type": "sell", "volume": self.makingvolume, "price": askPrice * 1.01}]
        for n in [0, 1]:
            print("try to create dex bid order: %s" % BidOrder[n])
            print(self.executeOrder("dex", BidOrder[n]))
            print("try to create dex ask order: %s" % AskOrder[n])
            print(self.executeOrder("dex", AskOrder[n]))
        self.currentDEXMiddlePrice = middlePrice
        print("currentDexMiddlePrice = %s" % self.currentDEXMiddlePrice)
        return

    async def run(self):
        while True:
            try:
                btc38Ticker = self.client.btc38Client.getTickers()['ticker']
                self.currentDEXMiddlePrice = (btc38Ticker["buy"] + btc38Ticker["sell"]) / 2
                while True:
                    if (self.clearTicker()):
                        self.generateMakerOrder()
                    else:
                        print("now there is no chance for arbitrage,  %s" % datetime.now())
                        await asyncio.sleep(5)
            except:
                print("some error happened")



class DataProcess():
    def __init__(self):
        self.client = TradeClient()


    def strUTC2strBJTime(self,utime):
        UTCTime = datetime.strptime(utime,'%Y-%m-%dT%H:%M:%S')
        BJTime = UTCTime + timedelta(hours=8)
        return datetime.strftime(BJTime,'%Y-%m-%d %H:%M:%S')


    def updateDatabase(self):
        #timely fetch data from btc38 and dex and write to database
        dexdata = self.client.btsClient.returnTradeHistory("BTS_CNY",limit=200)["BTS_CNY"]
        btc38data =[]
        pages=2
        for n in list(range(pages)):
                btc38data.append(self.client.btc38Client.getMyTradeList(page=n))

        try:
            with self.client.mysqlClient.cursor() as cursor:
                for record in dexdata:
                    record["date"]=self.strUTC2strBJTime(record["date"])
                    initialdata = record["date"]+record["type"]+str(record["amount"])+str(record["total"])
                    md5 = hashlib.md5()
                    md5.update(initialdata.encode("utf-8"))
                    hashid = md5.hexdigest()
                    paramstr = "('%s', '%s', '%s', '%f', '%f', '%s', '%s')" % (
                        hashid, 'dex',  'bts',record['rate'], float(record['amount']), record['date'],record['type'])
                    sql = "INSERT INTO `botdb` (`id`,`exchange`,`asset`,`price`,`volume`,`time`,`type`) VALUES " + paramstr + "ON DUPLICATE KEY UPDATE `id` = '%s'" % hashid
                    print(sql)
                    cursor.execute(sql)
                    self.client.mysqlClient.commit()

                for n in list(range(pages)):
                    for record in btc38data[n]:
                        if record["buyer_id"] == "3664":
                            record["type"] = "buy"
                        else:
                            record["type"] = "sell"

                        paramstr = "('%s', '%s', '%s', '%f', '%f', '%s', '%s')" % (
                            record['id'], 'btc38', record['coinname'],
                            float(record['price']), float(record['volume']), record['time'], record["type"])
                        sql = "INSERT INTO `botdb` (`id`,`exchange`,`asset`,`price`,`volume`,`time`,`type`) VALUES " + paramstr + "ON DUPLICATE KEY UPDATE `id` = '%s'" % record['id']

                        print(sql)

                        cursor.execute(sql)
                        print(self.client.mysqlClient.commit())

        finally:
            pass

async def DataUpdate():
    while True:
        dataprocesser = DataProcess()
        dataprocesser.updateDatabase()
        await asyncio.sleep(300)

if __name__ == "__main__":
    maker = MarketMaker()
    loop = asyncio.get_event_loop()
    tasks = [maker.run(), DataUpdate()]

    loop.run_until_complete(asyncio.wait(tasks))
    loop.close()

