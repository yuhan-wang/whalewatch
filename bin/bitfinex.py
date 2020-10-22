import collections

import kafka
from bfxapi import Client
from order_book import OrderBook
from order_book import kafka_send

bfx = Client()
exchange = "Bitfinex"

host = ['localhost:9092']
producer = kafka.KafkaProducer(bootstrap_servers=host)
kafka.KafkaClient(bootstrap_servers=host).add_topic('all')

with open('./trading_pairs/bitfinex.pair', 'r') as f:
    pairs = [e.replace('\n', '') for e in f.readlines()]
pairs = list(map(lambda x: 't' + x, pairs))
local_book = collections.defaultdict(OrderBook)


@bfx.ws.on('error')
def log_error(err):
    print("Error: {}".format(err))


@bfx.ws.on('order_book_update')
def log_update(data):
    ob = local_book[data['symbol']]
    order_change = ob.update_order(data['data'])

    if order_change:
        kafka_send(producer, 'all', exchange, data['symbol'], order_change)


@bfx.ws.on('order_book_snapshot')
def log_snapshot(data):
    ob = local_book[data['symbol']] = OrderBook()
    ob.initialize_book('bitfinex', bfx.ws.orderBooks[data['symbol']].bids, bfx.ws.orderBooks[data['symbol']].asks)


async def start():
    for i, pair in enumerate(pairs):
        await bfx.ws.subscribe('book', pair, prec='P0', len='100')


bfx.ws.on('connected', start)
bfx.ws.run()
