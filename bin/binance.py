import asyncio
import collections
import sys
import time
from json import loads

import aiohttp
import kafka
from order_book import OrderBook
from order_book import kafka_send
from unicorn_binance_websocket_api.unicorn_binance_websocket_api_manager import BinanceWebSocketApiManager


class BinanceOrderBook(OrderBook):
    def __init__(self, lastUpdateId=0):
        self.lastUpdateId = lastUpdateId
        super().__init__()


host = 'localhost:9092'
producer = kafka.KafkaProducer(bootstrap_servers=host)
kafka.KafkaClient(bootstrap_servers=host).add_topic('all')

exchange = 'binance.com'
manager = BinanceWebSocketApiManager(exchange=exchange)

with open('./trading_pairs/binance.pair', 'r') as f:
    pairs = [e.replace('\n', '') for e in f.readlines()]

local_book = collections.defaultdict(BinanceOrderBook)


# retrieve orderbook snapshot
async def get_snapshot(pair, session):
    async with session.get(f'https://www.binance.com/api/v1/depth?symbol={pair}&limit=100') as r:
        if r.status == 429:
            time.sleep(60)
        content = await r.json()
    return content


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


async def consume_update(data, lock, session):
    base_quote = data['s']

    # check for orderbook, if empty retrieve
    if base_quote not in local_book:
        async with lock:
            raw_book = await get_snapshot(base_quote, session)

        ob = local_book[base_quote] = BinanceOrderBook(raw_book['lastUpdateId'])
        ob.initialize_book('binance', raw_book['bids'], raw_book['asks'])

    # get lastUpdateId
    ob = local_book[base_quote]
    lastUpdateId = ob.lastUpdateId

    # maintain local order book according to Binance API
    if data['U'] <= lastUpdateId + 1 <= data['u']:
        ob.lastUpdateId = data['u']

        process_updates(ob, data)

    elif data['U'] > lastUpdateId + 1:
        del local_book[base_quote]


async def evt_loop(locks, session):
    asyncio.create_task(evt_loop(locks, session))
    payload = manager.pop_stream_data_from_stream_buffer()

    if payload:
        raw_data = loads(payload)
        if 'data' in raw_data:
            data = raw_data['data']
            await consume_update(data, locks[data['s']], session)


manager.create_stream('depth@100ms', pairs)
try:
    locks = {pair: asyncio.Lock() for pair in pairs}
    manager.create_stream('depth@100ms', pairs)
    async with aiohttp.ClientSession() as session:
        loop = asyncio.get_event_loop()
        loop.create_task(evt_loop(locks, session))
        loop.run_forever()
except KeyboardInterrupt:
    manager.stop_manager_with_all_streams()
    sys.exit(0)
