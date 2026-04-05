import hmac, hashlib, json, time
import time
# pip install websocket_client
from websocket import create_connection

def gen_sign(channel, event, timestamp):
    # GateAPIv4 key pair
    api_key = 'bab7ccee7586a59312d7b5bacf182e97'
    api_secret = 'aa0fea4b2d1df265595bea44798fe8809e3718475a1d584e698470dc771e0b81'

    s = 'channel=%s&event=%s&time=%d' % (channel, event, timestamp)
    sign = hmac.new(api_secret.encode('utf-8'), s.encode('utf-8'), hashlib.sha512).hexdigest()
    return {'method': 'api_key', 'KEY': api_key, 'SIGN': sign}


# ws = create_connection("wss://api.gateio.ws/ws/v4/")
# ws.send('{"time": %d, "channel" : "spot.ping"}' % int(time.time()))
# print(ws.recv())

ws = create_connection("wss://api.gateio.ws/ws/v4/")
request = {
    "time": int(time.time()),
    "channel": "spot.balances",
    "event": "subscribe",  # "unsubscribe" for unsubscription
}
# refer to Authentication section for gen_sign implementation
request['auth'] = gen_sign(request['channel'], request['event'], request['time'])
print(json.dumps(request))
ws.send(json.dumps(request))
print(ws.recv())
while(True):
    ret = ws.recv()
    if ret:
        print(ret)