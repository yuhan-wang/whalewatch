import collections
import sys
from pathlib import Path
from os import environ
from json import loads
from datetime import datetime
import time

from pytz import utc
from unicorn_binance_websocket_api.unicorn_binance_websocket_api_manager import BinanceWebSocketApiManager
import kafka
import requests

from order_book import kafka_send
from order_book import OrderBook


class BinanceOrderBook(OrderBook):
    def __init__(self, lastUpdateId=0):
        self.lastUpdateId = lastUpdateId
        super().__init__()


# retrieve orderbook snapshot
def get_snapshot(pair):
    r = requests.get(f'https://www.binance.com/api/v1/depth?symbol={pair}&limit=100')
    print(r.content)
    print(r.content.decode())
    return loads(r.content.decode())


exchange = 'binance.com'
manager = BinanceWebSocketApiManager(exchange=exchange)

with open('./trading_pairs/binance.pair', 'r') as f:
    pairs = [e.replace('\n', '') for e in f.readlines()]

manager.create_stream('depth@100ms', pairs[:10])
host = 'localhost:9092'
producer = kafka.KafkaProducer(bootstrap_servers=host)
kafka.KafkaClient(bootstrap_servers=host).add_topic('all')
c = 0
startTime = time.time()

local_book = collections.defaultdict(BinanceOrderBook)


def process_updates(ob, data):
    update_bids = data['b']
    update_asks = data['a']
    t = data['E']
    for update in update_bids:
        p = float(update[0])
        q = float(update[1])
        delta = ob.update_order([p, 1, q]) if q != 0 else ob.update_order([p, 0, 1])
        # print(data['s'], delta)
        if delta:
            kafka_send(producer, 'all', 'binance', data['s'], delta, timestamp=t)

    for update in update_asks:
        p = float(update[0])
        q = -float(update[1])
        delta = ob.update_order([p, 1, q]) if q != 0 else ob.update_order([p, 0, -1])
        # print(data['s'], delta)
        if delta:
            kafka_send(producer, 'all', 'binance', data['s'], delta, timestamp=t)


try:
    while True:
        payload = manager.pop_stream_data_from_stream_buffer()
        if payload:
            # print(payload)
            raw_data = loads(payload)
            if 'data' not in raw_data:
                continue
            data = loads(payload)['data']

            # update_bids = data['b']
            # updated_asks = data['a']
            basequote = data['s']
            # check for orderbook, if empty retrieve
            initial_flag = False
            if basequote not in local_book:
                raw_book = get_snapshot(basequote)
                ob = local_book[basequote] = BinanceOrderBook(raw_book['lastUpdateId'])
                ob.initialize_book('binance', raw_book['bids'], raw_book['asks'])
                # initial_flag = True

            # get lastUpdateId
            ob = local_book[basequote]
            lastUpdateId = ob.lastUpdateId

            if data['U'] <= lastUpdateId + 1 <= data['u']:
                print(f'lastUpdateId {data["u"]}')
                ob.lastUpdateId = data['u']
                process_updates(ob, data)
            elif data['U'] > lastUpdateId + 1:
                print("need resync")
                # del local_book[basequote]
            else:
                print("slow webs")
                print(data['U'], data['u'], data['s'], ob.lastUpdateId)
                # del local_book[basequote]

            # for p, q in update_bids:
            #     # print(p,q)
            #     c += 1
            #     if c == 1000:
            #         endTime = time.time()
            #         diff = endTime - startTime
            #         startTime = endTime
            #         print(diff, c / diff)
            #         c = 0
            #     key = ','.join((str(t), basequote, exchange)).encode()
            #     value = ','.join((p, q)).encode()
            # for p, q in updated_asks:
            #     # print(p,q)
            #     c += 1
            #     if c == 1000:
            #         endTime = time.time()
            #         diff = endTime - startTime
            #         startTime = endTime
            #         print(diff, c / diff)
            #         c = 0
            # key = ','.join((str(t), basequote, exchange)).encode()
            # value = ','.join((p, q)).encode()
            # print(key)
            # producer.send('all', key=key, value=value)
except KeyboardInterrupt:
    manager.stop_manager_with_all_streams()
    sys.exit(0)
