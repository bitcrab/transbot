from grapheneexchange import GrapheneExchange
#import config

class Config():
    witness_url           ="wss://bitshares.dacplay.org:8089/ws"
    witness_user          = ""
    witness_password      = ""

    watch_markets = ["CNY_BTS", "BTS_CNY"]
    market_separator = "_"
    account = "transwiser.test"
    wif = "5KfSRygyqDfUyeUfSbFt8qM7XNA4iGb35QogRJAYza6Z9erPiK8"

dex = GrapheneExchange(config, safe_mode=False)
#: Close all orders that have been put in those markets previously
orders = dex.returnOpenOrders()
for m in orders:
    for o in orders[m]:
        print(" - %s" % o["orderNumber"])
        dex.cancel(o["orderNumber"])
#: Buy and Sell Prices
buy_price  = 1 - config.bridge_spread_percent / 200
sell_price = 1 + config.bridge_spread_percent / 200
#: Amount of Funds available for trading (per asset)
balances = dex.returnBalances()
asset_ids = []
amounts = {}
for market in config.watch_markets :
    quote, base = market.split(config.market_separator)
    asset_ids.append(base)
    asset_ids.append(quote)
assets_unique = list(set(asset_ids))
for a in assets_unique:
    if a in balances :
        amounts[a] = balances[a] * config.bridge_amount_percent / 100 / asset_ids.count(a)
for m in config.watch_markets:
    quote, base = m.split(config.market_separator)
    if quote in amounts :
        print(" - Selling %f %s for %s @%f" % (amounts[quote], quote, base, sell_price))
        dex.sell(m, sell_price, amounts[quote])
    if base in amounts :
        print(" - Buying %f %s with %s @%f" % (amounts[base], base, quote, buy_price))
        dex.buy(m, buy_price, amounts[base] * buy_price)