import json
import yunbi.client
import yunbi
import btc38
import btc38.client
import pymysql.cursors
from grapheneexchange import GrapheneExchange

f = open ("config.json", 'r')
config = json.loads(f.read())
f.close()
#print(config)

for client in config:

    if client['client'] == 'bts':
        class Config():
            pass
        btsConfig = Config()
        btsConfig.witness_url = client['WITNESS_URL']
        btsConfig.witnes_user = ""
        btsConfig.witness_password = ""
        btsConfig.watch_markets = ["CNY_BTS","BTS_CNY"]
        btsConfig.market_separator = "_"
        btsConfig.account = client['ACCOUNT']
        btsConfig.wif = client['SECRET_KEY']

        btsClient = GrapheneExchange(btsConfig, safe_mode=False)

    if client['client'] == 'yunbi':

        yunbiClient = yunbi.client.Client(client['ACCESS_KEY'], client['SECRET_KEY'])

    if client['client'] == 'btc38':

        btc38Client = btc38.client.Client(client['ACCESS_KEY'], client['SECRET_KEY'], client['ACCOUNT_ID'])

    if client['client'] == 'mysql':
        mysqlClient = pymysql.connect(host=client['host'],user = client['user'], password=client['password'], database = client['database'])


def marketmaker():
    a = btc38Client.getTickers()
    print(a)

marketmaker()

"""
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