import json
import yunbi.client
import yunbi
import btc38
import btc38.client
f = open ("config.json", 'r')
config = json.loads(f.read())
f.close()
#print(config)

for client in config:

    if client['client'] == 'bts':
        pass

    if client['client'] == 'yunbi':

        yunbiClient = yunbi.client.Client(client['ACCESS_KEY'], client['SECRET_KEY'])

    if client['client'] == 'btc38':

        btc38Client = btc38.client.Client(client['ACCESS_KEY'], client['SECRET_KEY'], client['ACCOUNT_ID'])

    if client['client'] == 'mysql':
        pass

btc38Client.getMyBalance()