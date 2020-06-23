import collections

from bfxapi import Client
from order_book import OrderBook
from order_book import kafka_send

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


# stats = collections.defaultdict(list)


@bfx.ws.on('error')
def log_error(err):
    print("Error: {}".format(err))


#
#
# c = [0]
# timed = [0, 0]
# stime = [time.time(), time.time()]


@bfx.ws.on('order_book_update')
def log_update(data):
    # c[0] += 1
    # stime[1] = time.time()
    # if stime[1] - stime[0] > 2:
    #     print(c[0], c[0] / (stime[1] - stime[0]), stime[1] - stime[0])
    #     stime[0] = stime[1]
    #     c[0] = 0
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
    ob = local_book[data['symbol']]
    order_change = ob.update_order(data['data'])
    # print(order_change)
    if order_change:
        kafka_send('all', exchange, data['symbol'], order_change)
    # k = data['symbol'].encode()
    # v = ",".join(map(str, data['data'])).encode()
    # producer.send('all', key=k, value=v)


@bfx.ws.on('order_book_snapshot')
def log_snapshot(data):
    ob = local_book[data['symbol']] = OrderBook()
    ob.initialize_book('bitfinex', bfx.ws.orderBooks[data['symbol']].bids, bfx.ws.orderBooks[data['symbol']].asks)
    # if running_data[1] / running_data[2] < (running_data[0] / running_data[2]) ** 2:
    # print("running", running_data)
    # print("bids", bfx.ws.orderBooks[data['symbol']].update_bids)
    # print("run", running_data, "bids", ob.bids, "asks", ob.asks)
    # print('initial_book')
    # print("Initial book: {}".format(data))
    # print(data['symbol'], bfx.ws.orderBooks[data['symbol']].asks)


async def start():
    # print(bfx.ws.orderBooks['tBTCUSD'].asks)
    # await bfx.ws.enable_flag(Flags.TIMESTAMP)
    # await bfx.ws.subscribe('book', 'tBTCUSD', prec='P0', len='25')
    # await bfx.ws.subscribe('book', 'tALGUSD', len='100')
    # timed[0] = time.time()
    for i, pair in enumerate(pairs):
        # print(i)
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
