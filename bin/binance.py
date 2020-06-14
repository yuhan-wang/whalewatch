import sys
from pathlib import Path
from os import environ
from json import loads
from datetime import datetime
import time

from pytz import utc
from unicorn_binance_websocket_api.unicorn_binance_websocket_api_manager import BinanceWebSocketApiManager
import kafka

exchange='binance.com'
manager = BinanceWebSocketApiManager(exchange=exchange)

with open('./trading_pairs/binance.pair', 'r') as f:
    pairs = [e.replace('\n', '') for e in f.readlines()]

manager.create_stream('depth@100ms', pairs)
host = 'localhost:9092'
producer = kafka.KafkaProducer(bootstrap_servers=host)
kafka.KafkaClient(bootstrap_servers=host).add_topic('all')
c = 0
startTime = time.time()
try:
    while True:
        payload = manager.pop_stream_data_from_stream_buffer()
        if payload:
            #print(payload)
            raw_data = loads(payload)
            if 'data' not in raw_data:
                continue
            data = loads(payload)['data']
            t = data['E']
            bids = data['b']
            asks = data['a']
            basequote = data['s']
            for p,q in bids:
                #print(p,q)
                c+=1
                if c == 1000:
                    endTime = time.time()
                    diff = endTime - startTime
                    startTime = endTime
                    print(diff, c / diff)
                    c = 0
                key = ','.join((str(t), basequote, exchange)).encode()
                value = ','.join((p, q)).encode()
            for p, q in asks:
                #print(p,q)
                c+=1
                if c == 1000:
                    endTime = time.time()
                    diff = endTime-startTime
                    startTime = endTime
                    print(diff, c/diff)
                    c = 0
                key = ','.join((str(t), basequote, exchange)).encode()
                value = ','.join((p, q)).encode()
                # producer.send('all', key=key, value=value)
except KeyboardInterrupt:
    manager.stop_manager_with_all_streams()
    sys.exit(0)