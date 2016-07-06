from grapheneexchange.exchange import GrapheneExchange
from pprint import pprint

#witness_url           = "wss://bitshares.openledger.info/ws"

class Config():
    witness_url           = "wss://bitshares.dacplay.org:8089/ws"
    witness_user          = ""
    witness_password      = ""

    watch_markets         = ["CNY_BTS","BTS_CNY"]
    market_separator      = "_"
    account               = "transwiser.test"
    wif                   = "5KfSRygyqDfUyeUfSbFt8qM7XNA4iGb35QogRJAYza6Z9erPiK8"

if not __name__ == '__main__':
    dex   = GrapheneExchange(Config, safe_mode=True)
    pprint((dex.returnTradeHistory("CNY_BTS")))
    #pprint((dex.returnTradeHistory("BTS_CNY")))
    pprint((dex.returnTicker()))
    pprint((dex.return24Volume()))
    pprint((dex.returnOrderBook("CNY_BTS")))
    pprint((dex.returnOrderBook("BTS_CNY")))
    pprint((dex.returnBalances()))
    pprint((dex.returnOpenOrders("all")))
    pprint(dex.buy("CNY_BTS", 33, 10))
    pprint(dex.sell("CNY_BTS",33, 10))
    #pprint(dex.close_debt_position("CNY"))
    pprint(dex.adjust_debt(1, "CNY", 3.0))
    pprint(dex.borrow(1, "CNY", 3.0))
    pprint(dex.cancel("1.7.1111"))

dex = GrapheneExchange(Config, safe_mode=True)
#pprint(dex.close_debt_position("CNY"))
#pprint(dex.adjust_debt(1, "CNY", 2))
#pprint(dex.buy("CNY_BTS", 33, 10))
#pprint(dex.borrow(1, "CNY", 2))

print(dex.rpc.info())
# Calls to the witness node
print(dex.ws.get_account("init0"))
print(dex.ws.get_asset("USD"))
print(dex.ws.get_account_count())