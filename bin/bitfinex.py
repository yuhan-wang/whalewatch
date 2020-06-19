"""

Need to implement own code to get new order or manage an order book in spark
"""
import collections


class OrderBook:
    def __init__(self):
        self.bids = []
        self.asks = []


from bfxapi import Client
import kafka

import time

# producer_timings = {}
# def calculate_thoughput(timing, n_messages=1000000, msg_size=100):
#     print("Processed {0} messsages in {1:.2f} seconds".format(n_messages, timing))
#     print("{0:.2f} MB/s".format((msg_size * n_messages) / timing / (1024*1024)))
#     print("{0:.2f} Msgs/s".format(n_messages / timing))

bfx = Client()
# bfx_agg = Client()
exchange = "Bitfinex"
# bfx = Client(logLevel='DEBUG')

host = ['localhost:9092']
# producer = kafka.KafkaProducer(bootstrap_servers=host)
# kafka.KafkaClient(bootstrap_servers=host).add_topic('all')

with open('./trading_pairs/bitfinex.pair', 'r') as f:
    pairs = [e.replace('\n', '') for e in f.readlines()]
pairs = list(map(lambda x: 't' + x, pairs))
local_book = collections.defaultdict(OrderBook)
stats = collections.defaultdict(list)


@bfx.ws.on('error')
def log_error(err):
    print("Error: {}".format(err))


def kafka_send(symbol, data):
    k = ",".join([exchange, symbol]).encode()
    v = ",".join(map(str, data)).encode()
    # print(v)
    producer.send('all', key=k, value=v)


def update_order(data):
    # delta = [price, count, quantity, (bid/ask) flag, best_bid, best_ask, total_bid_amount, total_ask_amount, avg, var]
    # key -1 indicates ask, 1 indicates bid
    # total_amount = {-1: 0, 1: 0}
    # running_data = [running_total, running_squared_total, running_count] (of new orders)

    p, count, q = data['data']
    book = local_book[data['symbol']]
    total_amount = stats[data['symbol']][0]
    running_data = stats[data['symbol']][1]

    if q < 0:
        flag = -1
        side = book.asks
        q = -q
    else:
        flag = 1
        side = book.bids
    best_bid = book.bids[0][0] if book.bids else 0
    best_ask = book.asks[0][0] if book.asks else float("inf")
    delta = [p, count, q, flag, best_bid, best_ask, total_amount[1], total_amount[-1], 0, 0]
    for index, info in enumerate(side):
        if info[0] == p:

            delta[1] -= info[1]
            if count == 0:
                delta[2] = -info[2]
                total_amount[flag] += delta[2]
                del side[index]
                if delta[8] <= 0 or delta[9] <= 0:
                    print(delta, q, count, data)
                return delta
            del side[index]
            delta[2] -= info[2]
            # print(side)
            break
    # if price level not present in the order book
    if count == 0:
        return []
    total_amount[flag] += delta[2]
    # avg
    delta[8] = running_data[0] / running_data[2]
    # var
    delta[9] = running_data[1] / running_data[2] - delta[8] ** 2

    if delta[2] > 0:
        running_data[2] += 1
        running_data[0] += delta[2]
        running_data[1] += delta[2] ** 2
    side.append([p, count, q])
    side.sort(key=lambda x: x[0], reverse=not flag < 0)
    if delta[8] <= 0 or delta[9] <= 0:
        print(delta, q, count, data)
    return delta


c = [0]
timed = [0, 0]
stime = [time.time(), time.time()]


@bfx.ws.on('order_book_update')
def log_update(data):
    c[0] += 1
    stime[1] = time.time()
    if stime[1] - stime[0] > 2:
        print(c[0], c[0] / (stime[1] - stime[0]), stime[1] - stime[0])
        stime[0] = stime[1]
        c[0] = 0
    # if c[0] == 6000:
    #     timed[1] = time.time()
    #     print(timed[1] - timed[0], c[0] / (timed[1] - timed[0]))
    #     c[0] = 0
    #     timed[0] = timed[1]

    # print(bfx.ws.orderBooks[data['symbol']].asks)
    # print(bfx.ws.orderBooks['tBTCUSD'].asks)
    # print("Book update: {}".format(data))
    # print(type(data), sys.getsizeof(data)+sys.getsizeof(data['symbol'])+sys.getsizeof(data['data']))

    # if q<0:
    #     side=bfx.ws.orderBooks[data['symbol']].asks
    # else:
    #     side=bfx.ws.orderBooks[data['symbol']].bids
    # print(bfx.ws.orderBooks[data['symbol']].asks)
    # print(data)
    # data error for chanId=91485, disappeared today
    # if data['data'][0]==91485:
    #     print(data['symbol'], data['data'])
    # print(data)
    order_change = update_order(data)
    # if order_change:
    #     kafka_send(data['symbol'], order_change)
    # k = data['symbol'].encode()
    # v = ",".join(map(str, data['data'])).encode()
    # producer.send('all', key=k, value=v)


@bfx.ws.on('order_book_snapshot')
def log_snapshot(data):
    ob = local_book[data['symbol']] = OrderBook()
    total_amount = {}
    running_data = [0, 0, 0]
    stats[data['symbol']] = [total_amount, running_data]
    ob.bids = [x[0].copy() for x in bfx.ws.orderBooks[data['symbol']].bids]
    ob.asks = [[x[0][0], x[0][1], -x[0][2]] for x in bfx.ws.orderBooks[data['symbol']].asks]
    ob.bids.sort(key=lambda x: x[0], reverse=True)
    ob.asks.sort(key=lambda x: x[0])

    total_amount[1] = sum(x[2] for x in ob.bids)
    total_amount[-1] = sum(x[2] for x in ob.asks)
    running_data[0] += sum(total_amount.values())
    running_data[1] += sum(x[2] ** 2 for x in ob.bids) + sum(x[2] ** 2 for x in ob.asks)
    running_data[2] += sum(x[1] for x in ob.bids) + sum(x[1] for x in ob.asks)
    if running_data[1] / running_data[2] < (running_data[0] / running_data[2]) ** 2:
        print("running", running_data)
    print("bids", bfx.ws.orderBooks[data['symbol']].bids)
    print("run", running_data, "bids", ob.bids, "asks", ob.asks)
    # print('initial_book')
    # print("Initial book: {}".format(data))
    # print(data['symbol'], bfx.ws.orderBooks[data['symbol']].asks)


async def start():
    # print(bfx.ws.orderBooks['tBTCUSD'].asks)
    # await bfx.ws.enable_flag(Flags.TIMESTAMP)
    # await bfx.ws.subscribe('book', 'tBTCUSD', prec='P0', len='25')
    # await bfx.ws.subscribe('book', 'tALGUSD', len='100')
    timed[0] = time.time()
    for i, pair in enumerate(pairs):
        print(i)
        await bfx.ws.subscribe('book', pair, prec='P0', len='100')
    # await bfx_agg.ws.subscribe('book', pair, prec='P0', len='100')


bfx.ws.on('connected', start)
bfx.ws.run()
#
# producer = kafka.KafkaProducer(bootstrap_servers=host)
# kafka.KafkaClient(bootstrap_servers=host).add_topic('all')
#
# @bfx.ws.on('error')
# def log_error(err):
#     print("Error: {}".format(err))
#
# @bfx.ws.on('order_book_snapshot')
# def log_snapshot(data):
#     print ("Initial book: {}".format(data))
#     print(bfx.ws.orderBooks[data['symbol']].asks)
#
#
# @bfx.ws.on('order_book_update')
# def log_update(data):
#     print("Book update: {}".format(data))
#     producer.send('all', value=data)
#
# @bfx.ws.on('connected')
# async def start():
#     await bfx.ws.subscribe('book', 'tBTCUSD')
#
# bfx.ws.run()

# kafka host = PLAINTEXT://ec2-54-224-244-135.compute-1.amazonaws.com:9092
