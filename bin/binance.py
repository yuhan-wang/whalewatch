import collections
import sys
from json import loads

import kafka
import requests
from order_book import OrderBook
from order_book import kafka_send
from unicorn_binance_websocket_api.unicorn_binance_websocket_api_manager import BinanceWebSocketApiManager


class BinanceOrderBook(OrderBook):
    def __init__(self, lastUpdateId=0):
        self.lastUpdateId = lastUpdateId
        super().__init__()


# retrieve orderbook snapshot
def get_snapshot(pair):
    r = requests.get(f'https://www.binance.com/api/v1/depth?symbol={pair}&limit=100')
    return loads(r.content.decode())


exchange = 'binance.com'
manager = BinanceWebSocketApiManager(exchange=exchange)

with open('./trading_pairs/binance.pair', 'r') as f:
    pairs = [e.replace('\n', '') for e in f.readlines()]

manager.create_stream('depth@100ms', pairs)
host = 'localhost:9092'
producer = kafka.KafkaProducer(bootstrap_servers=host)
kafka.KafkaClient(bootstrap_servers=host).add_topic('all')

local_book = collections.defaultdict(BinanceOrderBook)


def process_updates(ob, data):
    update_bids = data['b']
    update_asks = data['a']
    t = data['E']
    for update in update_bids:
        p = float(update[0])
        q = float(update[1])
        delta = ob.update_order([p, 1, q]) if q != 0 else ob.update_order([p, 0, 1])
        if delta:
            kafka_send(producer, 'all', 'binance', data['s'], delta, timestamp=t)

    for update in update_asks:
        p = float(update[0])
        q = -float(update[1])
        delta = ob.update_order([p, 1, q]) if q != 0 else ob.update_order([p, 0, -1])
        if delta:
            kafka_send(producer, 'all', 'binance', data['s'], delta, timestamp=t)


try:
    while True:
        payload = manager.pop_stream_data_from_stream_buffer()
        if payload:
            raw_data = loads(payload)
            if 'data' not in raw_data:
                continue
            data = loads(payload)['data']

            basequote = data['s']
            # check for orderbook, if empty retrieve
            if basequote not in local_book:
                raw_book = get_snapshot(basequote)
                ob = local_book[basequote] = BinanceOrderBook(raw_book['lastUpdateId'])
                ob.initialize_book('binance', raw_book['bids'], raw_book['asks'])

            # get lastUpdateId
            ob = local_book[basequote]
            lastUpdateId = ob.lastUpdateId

            # maintain local order book according to Binance API
            if data['U'] <= lastUpdateId + 1 <= data['u']:
                ob.lastUpdateId = data['u']
                process_updates(ob, data)
            elif data['U'] > lastUpdateId + 1:
                del local_book[basequote]

except KeyboardInterrupt:
    manager.stop_manager_with_all_streams()
    sys.exit(0)
