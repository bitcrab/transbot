from grapheneexchange import GrapheneExchange
import json


class Config():
    witness_url           ="wss://bitshares.dacplay.org:8089/ws"
    witness_user          = ""
    witness_password      = ""

    watch_markets = ["CNY_BTS", "BTS_CNY"]
    market_separator = "_"
    account = "transwiser.test"
    wif = "5KfSRygyqDfUyeUfSbFt8qM7XNA4iGb35QogRJAYza6Z9erPiK8"

if __name__ == '__main__':
    dex   = GrapheneExchange(Config)
    print(json.dumps(dex.returnTradeHistory("CNY_BTS"),indent=4))
    print(json.dumps(dex.returnTicker(),indent=4))
    print(json.dumps(dex.return24Volume(),indent=4))
    print(json.dumps(dex.returnOrderBook("CNY_BTS"),indent=4))
    print(json.dumps(dex.returnBalances(),indent=4))
    print(json.dumps(dex.returnOpenOrders("all"),indent=4))
    print(json.dumps(dex.buy("CNY_BTS", 33, 10),indent=4))
    print(json.dumps(dex.sell("CNY_BTS", 33, 10),indent=4))
    print(json.dumps(dex.adjust_debt(1, "CNY", 3.0), indent=4))
    #print(json.dump(dex.borrow(1, "CNY", 3.0), indent=4))