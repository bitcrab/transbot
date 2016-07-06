import json
import yunbi

f = open ("config.json", 'r')
config = json.loads(f.read())
f.close()
print(config)

for client in config:

    if client['client'] == 'bts':
        pass

    if client['client'] == 'yunbi':
        yunbiClient = yunbi.client.Client(client['ACCESS_KEY'], client['SECRET_KEY'])

    if client['client'] == 'btc38':
        pass

    if client['client'] == 'mysql':
        pass
