import json
import yunbi.client
import yunbi
import btc38
import btc38.client
import pymysql.cursors
from grapheneexchange import GrapheneExchange
import time
from datetime import datetime, timedelta
import hashlib
import sys


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

                self.btsConfig = btsConfig

                self.btsClient = GrapheneExchange(btsConfig, safe_mode=False)

            if client['client'] == 'yunbi':
                self.yunbiClient = yunbi.client.Client(client['ACCESS_KEY'], client['SECRET_KEY'])

            if client['client'] == 'btc38':
                self.btc38Client = btc38.client.Client(client['ACCESS_KEY'], client['SECRET_KEY'], client['ACCOUNT_ID'])

            if client['client'] == 'mysql':
                self.mysqlClient = pymysql.connect(host=client['host'], user=client['user'],
                                                   password=client['password'],
                                                   database=client['database'])
    def renewDEXconn(self):
        self.btsClient = GrapheneExchange(self.btsConfig, safe_mode=False)

class MarketMaker(object):
    def __init__(self):
        self.client = TradeClient()
        self.currentmiddlePrice = {"dex":0, "yunbi":0}
        self.makingvolume = 60000

    def checkBalance(self,exchanges=["dex","btc38","yunbi"],limit={"BTS":1000000,"CNY":20000}):
        checkResult = True
        balance = {"btc38":{"CNY":0,"BTS":0},"dex":{"CNY":0,"BTS":0},"yunbi":{"CNY":0,"BTS":0}}
        for ex in exchanges:
            if ex == "btc38":
                result = self.client.btc38Client.getMyBalance()
                balance["btc38"]["CNY"] = float(result["cny_balance"])
                balance["btc38"]["BTS"] = float(result["bts_balance"])
            if ex =="dex":
                result = self.client.btsClient.returnBalances()
                balance["dex"]["BTS"] = result["BTS"]
                balance["dex"]["CNY"] = result["CNY"]
            if ex == "yunbi":
                result = self.client.yunbiClient.getBalance()
                balance["yunbi"]["BTS"] = result["bts"]
                balance["yunbi"]["CNY"] = result["cny"]
        for ex in exchanges:
            for asset in ["BTS","CNY"]:
                if balance[ex][asset] < limit[asset]:
                    print("%s amount in %s is not enough" % (asset, ex))
                    checkResult =False
        return checkResult

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
            if Order["market"] =="BTS_CNY":
                # below is to handle the trouble brought by precision while taking orders
                if (Order["volume"]*100000)%1 != 0:
                    Order["volume"] = round(Order["volume"],5)+0.00001
                if  Order["price"]*Order["volume"] < 0.0001:
                    Order["volume"] = round(0.00011/Order["price"],5)
            if Order["type"] == "buy":
                return json.dumps(self.client.btsClient.buy("BTS_CNY", Order["price"], Order["volume"]))
            if Order["type"] == "sell":
                return json.dumps(self.client.btsClient.sell("BTS_CNY", Order["price"], Order["volume"]))

        if exchange == "yunbi":
            params = {'market': 'btscny', 'side': Order["type"], 'volume': Order["volume"], 'price': Order["price"]}
            res = self.client.yunbiClient.post('orders', params)
            print(res)


    def cancelAllOrders(self, exchanges=['dex'], quote="bts"):
        for ex in exchanges:
            if ex == "dex":
                orders = self.client.btsClient.returnOpenOrders("BTS_CNY")['BTS_CNY']
                for order in orders:
                    print("DEX order canceled:")
                    print(self.client.btsClient.cancel(order["orderNumber"]))
            if ex == "btc38":
                orders = self.client.btc38Client.getOrderList("bts")
                for order in orders:
                    print("btc38 order canceled:")
                    print(self.client.btc38Client.cancelOrder("cny", order["id"]))
            if ex=="yunbi":
                orders = self.client.yunbiClient.get('orders', {'market': 'btscny'}, True)
                for order in orders:
                    print("yunbi order canceled:")
                    params = {"id": order["id"]}
                    print(self.client.yunbiClient.post('delete_order', params))
        return

    def fetchMarketInfo(self):
        btc38Ticker = self.client.btc38Client.getTickers()['ticker']
        dexTicker = self.dexTicker2General(self.client.btsClient.returnTicker()['BTS_CNY'])
        yunbiTicker = self.client.yunbiClient.getTickers()

        btc38OrderBook = self.client.btc38Client.getDepth()
        dexOrderBook = self.client.btsClient.returnOrderBook("BTS_CNY")['BTS_CNY']
        yunbiOrderBook = self.client.yunbiClient.getOrderBook()

        dexOpenOrders = self.client.btsClient.returnOpenOrders("BTS_CNY")["BTS_CNY"]
        yunbiOpenOrders = self.client.yunbiClient.getOpenOrders()
        marketInfo = [{"exname": "dex", "ticker": dexTicker, "orderbook": dexOrderBook,"openorders":dexOpenOrders},
                      {"exname": "btc38", "ticker": btc38Ticker, "orderbook": btc38OrderBook},
                      {"exname": "yunbi", "ticker": yunbiTicker, "orderbook": yunbiOrderBook, "openorders":yunbiOpenOrders}
                      ]
        return marketInfo


    def clearTicker(self, exchanges=['dex', 'btc38']):
        marketInfo = self.fetchMarketInfo()
        for member in marketInfo:
            if member["exname"] == 'btc38':
                middlePrice = (member["ticker"]["buy"]+member["ticker"]["sell"])/2

        minGap = middlePrice * 0.008
        loop=0
        while True:
            loop+=1
            print("begin the arbitrage chance check circle, %s loop" % loop)

            askList = sorted(marketInfo,key=lambda x:x["ticker"]["sell"])
            bidList = sorted(marketInfo, key=lambda x:x["ticker"]["buy"], reverse=True)

            if bidList[0]["orderbook"]["bids"][0][0] > askList[0]["orderbook"]["asks"][0][0]:#check if arbitrage chance exist
                self.cancelAllOrders(["dex", "yunbi"])#cancel orders and check again
                print("have removed the orders with potential to be arbitraged!")
                time.sleep(2)
                try:
                    marketInfo = self.fetchMarketInfo()
                except:
                    print("fetchMarketInfo not executed correctly")

                askList = sorted(marketInfo, key=lambda x: x["ticker"]["sell"])
                bidList = sorted(marketInfo, key=lambda x: x["ticker"]["buy"], reverse=True)
                if bidList[0]["orderbook"]["bids"][0][0] > askList[0]["orderbook"]["asks"][0][0]:
                    #generate orders and execute for arbitrage
                    BidOrder = {"market":"BTS_CNY","type": "buy", "volume": askList[0]["orderbook"]["asks"][0][1],
                                "price": askList[0]["orderbook"]["asks"][0][0], "index": 0}
                    AskOrder = {"market":"BTS_CNY","type": "sell", "volume": bidList[0]["orderbook"]["bids"][0][1],
                                "price": bidList[0]["orderbook"]["bids"][0][0], "index": 0}
                    while bidList[0]["orderbook"]["bids"][AskOrder['index']][0] > (
                        askList[0]["orderbook"]["asks"][BidOrder["index"]][0] + minGap):
                        # BidOrder["price"] < (AskOrder["price"] - minGap):
                        pointBidOrder = BidOrder["volume"] > AskOrder["volume"]
                        if pointBidOrder:
                            AskOrder["index"] += 1
                            if bidList[0]["orderbook"]["bids"][AskOrder['index']][0] > (BidOrder["price"] + minGap):
                                AskOrder["volume"] += bidList[0]["orderbook"]["bids"][AskOrder['index']][1]
                                AskOrder["price"] = bidList[0]["orderbook"]["bids"][AskOrder['index']][0]
                        else:
                            BidOrder["index"] += 1
                            if askList[0]["orderbook"]["asks"][BidOrder["index"]][0] < (AskOrder["price"] - minGap):
                                BidOrder["volume"] += askList[0]["orderbook"]["asks"][BidOrder["index"]][1]
                                BidOrder["price"] = askList[0]["orderbook"]["asks"][BidOrder["index"]][0]
                    BidOrder["volume"] = min(BidOrder["volume"], AskOrder["volume"])
                    AskOrder["volume"] = BidOrder["volume"]

                    if self.checkBalance([askList[0]["exname"]],{"BTS":0,"CNY": BidOrder["volume"]*BidOrder["price"] }) and self.checkBalance([bidList[0]["exname"]],{"BTS":AskOrder["volume"],"CNY":0}):
                        print("now try to sumit bid order for arbitrage in %s" % askList[0]["exname"])
                        print(BidOrder)
                        print(self.executeOrder(askList[0]["exname"], BidOrder))

                        print("now try to sumit ask order for arbitrage in %s" % bidList[0]["exname"])
                        print(AskOrder)
                        print(self.executeOrder(bidList[0]["exname"], AskOrder))
                    else:
                        print("no enough balance to arbitrage, just skip ")


                    #return 1
                else:
                    return 1

            else:

                for member in marketInfo:
                    if member["exname"] in ["dex","yunbi"]:#check whether the ex need market making
                        sumOpenOrderAmount = 0
                        for order in member["openorders"]:
                            sumOpenOrderAmount += order["amount"]
                        #marketmiddleprice = (member["ticker"]["sell"]+member["ticker"]["buy"])/2
                        priceshift = middlePrice - self.currentmiddlePrice[member["exname"]]
                        if (abs(priceshift) > minGap * 2) or (sumOpenOrderAmount < self.makingvolume * 3 ):


                            self.cancelAllOrders([member["exname"]])
                            print("deleted %s orders for regeneration as price shifted too much or not enough order volume, middle price = %s, price shift = %s, minGap = %s, left order volums = %s BTS." % (
                                    member["exname"], middlePrice, priceshift, minGap, sumOpenOrderAmount))
                            self.generateMakerOrder(member["exname"])
                return 0

    def generateMakerOrder(self, exchanges=['dex', 'yunbi']):
        btc38Ticker = self.client.btc38Client.getTickers()['ticker']
        middlePrice = (btc38Ticker["buy"] + btc38Ticker["sell"]) / 2
        if "dex" in exchanges:
            settlePrice = self.client.btsClient.returnTicker()['BTS_CNY']['settlement_price']
            bidPrice = max((btc38Ticker["buy"]), middlePrice * 0.995)
            askPrice = max(btc38Ticker["sell"],middlePrice*1.005)#max(settlePrice * 1.01, middlePrice * 1.012)
            BidOrder = [{"market":"BTS_CNY", "type": "buy", "volume": self.makingvolume, "price": bidPrice},
                        {"market":"BTS_CNY", "type": "buy", "volume": self.makingvolume, "price": bidPrice * 0.99}]
            AskOrder = [{"market":"BTS_CNY", "type": "sell", "volume": self.makingvolume * 0.8, "price": askPrice},
                        {"market":"BTS_CNY", "type": "sell", "volume": self.makingvolume, "price": askPrice * 1.01}]
            for n in [0, 1]:
                print("try to create dex bid order: %s" % BidOrder[n])
                print(self.executeOrder("dex", BidOrder[n]))
                print("try to create dex ask order: %s" % AskOrder[n])
                print(self.executeOrder("dex", AskOrder[n]))
            self.currentmiddlePrice["dex"] = middlePrice
            #self.currentdexMiddlePrice = middlePrice
            print("current middle prrice in dex = %s" % middlePrice)
        if "yunbi" in exchanges:
            bidPrice = max(btc38Ticker["buy"]*0.997, middlePrice * 0.995)
            askPrice = min(btc38Ticker["sell"]*1.005, middlePrice * 1.005)
            BidOrder = [{"type": "buy", "volume": self.makingvolume, "price": bidPrice},
                        {"type": "buy", "volume": self.makingvolume, "price": bidPrice * 0.99}]
            AskOrder = [{"type": "sell", "volume": self.makingvolume*0.7, "price": askPrice},
                        {"type": "sell", "volume": self.makingvolume, "price": askPrice * 1.01}]
            for n in [0, 1]:
                print("try to create yunbi bid order: %s" % BidOrder[n])
                print(self.executeOrder("yunbi", BidOrder[n]))
                print("try to create yunbi ask order: %s" % AskOrder[n])
                print(self.executeOrder("yunbi", AskOrder[n]))
            self.currentmiddlePrice["yunbi"] = middlePrice
            print("current middle prrice in yunbi = %s" % middlePrice)
        return


    def run(self):
        try:
            if (self.clearTicker()):
                self.generateMakerOrder()
            else:
                print("now there is no chance for arbitrage,  %s" % datetime.now())
                time.sleep(5)
        except ConnectionError as e:
            print("connection error while running marketmaker:", e)
        except EOFError as e:
            print("EOF error while running marketmaker:", e)
        except:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            traceback_details = {
                'filename': exc_traceback.tb_frame.f_code.co_filename,
                'lineno': exc_traceback.tb_lineno,
                'name': exc_traceback.tb_frame.f_code.co_name,
                'type': exc_type.__name__,
                'message': exc_value,
            }

            del (exc_type, exc_value, exc_traceback)
            print("error while running marketmaker:",traceback_details)
            if traceback_details["type"] == "RPCError":
                self.client.renewDEXconn()
                print("Renew the connection to DEX WS node")
            time.sleep(3)


class DataProcess(object):
    def __init__(self):
        self.client = TradeClient()

    def strUTC2strBJTime(self,utime):
        UTCTime = datetime.strptime(utime,'%Y-%m-%dT%H:%M:%S')
        BJTime = UTCTime + timedelta(hours=8)
        return datetime.strftime(BJTime,'%Y-%m-%d %H:%M:%S')

    def updateDatabase(self):
        dexdata = self.client.btsClient.returnTradeHistory("BTS_CNY",limit=100)["BTS_CNY"]
        btc38data =[]
        pages=2
        for n in list(range(pages)):
            btc38data.append(self.client.btc38Client.getMyTradeList(page=n))

        params = {'market': 'btscny', 'limit': 100}
        yunbidata =self.client.yunbiClient.get("my_trades", params, True)

        with self.client.mysqlClient.cursor() as cursor:
            for record in dexdata:
                record["date"] = self.strUTC2strBJTime(record["date"])
                initialdata = record["date"] + record["type"] + str(record["amount"]) + str(record["total"])
                md5 = hashlib.md5()
                md5.update(initialdata.encode("utf-8"))
                hashid = md5.hexdigest()
                paramstr = "('%s', '%s', '%s', '%f', '%f', '%s', '%s')" % (
                    hashid, 'dex', 'bts', record['rate'], float(record['amount']), record['date'], record['type'])
                sql = "INSERT INTO `botdb` (`id`,`exchange`,`asset`,`price`,`volume`,`time`,`type`) VALUES " + paramstr + "ON DUPLICATE KEY UPDATE `id` = '%s'" % hashid
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
                    sql = "INSERT INTO `botdb` (`id`,`exchange`,`asset`,`price`,`volume`,`time`,`type`) VALUES " + paramstr + "ON DUPLICATE KEY UPDATE `id` = '%s'" % \
                                                                                                                              record[
                                                                                                                                  'id']
                    cursor.execute(sql)
                    self.client.mysqlClient.commit()

            for record in yunbidata:
                type = "buy" if record["side"] == "bid" else "sell"
                paramstr = "('%s', '%s', '%s', '%f', '%f', '%s', '%s')" % (
                record["id"], 'yunbi', 'bts', float(record['price']), float(record['volume']),
                str(datetime.fromtimestamp(record['at'])), type)
                sql = "INSERT INTO `botdb` (`id`,`exchange`,`asset`,`price`,`volume`,`time`,`type`) VALUES " + paramstr + "ON DUPLICATE KEY UPDATE `id` = '%s'" % \
                                                                                                                          record[
                                                                                                                              "id"]
                cursor.execute(sql)
                self.client.mysqlClient.commit()

            today = datetime.now()
            tomorrow = today + timedelta(hours=24)

            strToday = str(today)[:10] + ' 00:00:00'
            strTomorrow = str(tomorrow)[:10] + ' 00:00:00'

            for ex in ['dex', 'btc38', 'yunbi']:
                initialdata = strToday + ex + "BTSCNY"
                md5 = hashlib.md5()
                md5.update(initialdata.encode("utf-8"))
                hashid = md5.hexdigest()

                paramstrbuy = "(SELECT SUM(`volume`) FROM botdb WHERE `exchange` = '%s' and `type` = 'buy' and `time` >= '%s' and time < '%s')" % (
                ex, strToday, strTomorrow)
                paramstrpaid = "(SELECT SUM(`volume`*`price`) FROM botdb WHERE `exchange` = '%s' and `type` = 'buy' and `time` >= '%s' and time < '%s')" % (
                ex, strToday, strTomorrow)
                paramstsell = "(SELECT SUM(`volume`) FROM botdb WHERE `exchange` = '%s' and `type` = 'sell' and `time` >= '%s' and time < '%s')" % (
                ex, strToday, strTomorrow)
                paramsreceived = "(SELECT SUM(`volume`*`price`) FROM botdb WHERE `exchange` = '%s' and `type` = 'sell' and `time` >= '%s' and time < '%s')" % (
                ex, strToday, strTomorrow)

                paramstr = "('%s', '%s', '%s', '%s', '%s', %s, %s, %s, %s)" % (
                hashid, ex, strToday, "BTS", "CNY", paramstrbuy, paramstrpaid, paramstsell, paramsreceived)
                sql = "REPLACE INTO `dailyreport`(`id`, `exchange`, `date`, `quote`, `base`, `buy`, `paid`, `sell`, `received`)VALUES" + paramstr
                print(sql)
                cursor.execute(sql)
                self.client.mysqlClient.commit()

            sql = "UPDATE `dailyreport` SET `netpaid` = `paid` - `received`, `netbuy` = `buy` - `sell`, `avebuyprice` = `paid`/`buy`, `avesellprice` = `received`/`sell`"
            print(sql)
            cursor.execute(sql)
            self.client.mysqlClient.commit()

    def run(self):
        try:
            self.updateDatabase()
        except ConnectionError as e:
            print("connection error while running data processing:", e)
        except EOFError as e:
            print("EOF error while running data processing:", e)
        except:
            exc_type, exc_value, exc_traceback = sys.exc_info()  # most recent (if any) by default
            traceback_details = {
                'filename': exc_traceback.tb_frame.f_code.co_filename,
                'lineno': exc_traceback.tb_lineno,
                'name': exc_traceback.tb_frame.f_code.co_name,
                'type': exc_type.__name__,
                'message': exc_value,  # or see traceback._some_str()
            }

            del (exc_type, exc_value, exc_traceback)  # So we don't leave our local labels/objects dangling
            print("unknow error while running data processing",traceback_details)

maker = MarketMaker()
#processer = DataProcess()
#processer.client = maker.client


while True:

    maker.fetchMarketInfo()
    print ("fetch data ended")
    time.sleep(8)

"""

while True:
    try:
        maker.checkBalance()
    except ConnectionError as e:
        print("connection error while checking Balance:", e)
    except EOFError as e:
        print("EOF error while checking Balance:", e)
    except:
        exc_type, exc_value, exc_traceback = sys.exc_info()  # most recent (if any) by default
        traceback_details = {
            'filename': exc_traceback.tb_frame.f_code.co_filename,
            'lineno': exc_traceback.tb_lineno,
            'name': exc_traceback.tb_frame.f_code.co_name,
            'type': exc_type.__name__,
            'message': exc_value,  # or see traceback._some_str()
        }

        del (exc_type, exc_value, exc_traceback)  # So we don't leave our local labels/objects dangling
        print("unknow error while checking balance:",traceback_details)
    for n in list(range(30)):
        print ("n=%s in 30 circles" % n)
        maker.run()
    processer.run()


maker = MarketMaker()
processer = DataProcess()
processer.client = maker.client

while True:
    for n in list(range(20)):
        maker.run()
    processer.run()

#yunbi = TradeClient().yunbiClient
dex = TradeClient().btsClient

print(json.dumps(dex.buy("BTS_CNY",0.037015000005220256,1000.446088027566)))
print(json.dumps(dex.returnOrderBook("BTS_CNY")['BTS_CNY'],indent=4))
#print(json.dumps(dex.buy("BTS_CNY",0.03717472118959108,0.00269)))
#print(json.dumps(dex.returnTradeHistory("BTS_OPEN.BTC")['BTS_OPEN.BTC'],indent=4))
#params = {'market': 'btscny','limit':5}
#print(json.dumps(yunbi.get("trades", {'market': 'btscny'},sigrequest = True),indent=4))



#print(json.dumps(yunbi.get('order_book',{'market': 'btscny'},True),indent=4))



#print(json.dumps(yunbi.get('tickers',{'market': 'btscny'}),indent=4))
#print(json.dumps(c.btc38Client.getDepth('bts'),indent=4))

#print(json.dumps(c.yunbiClient.get('orders',{'market': 'btscny'}, True),indent=4))
"""
