"""

Need to implement own code to get new order or manage an order book in spark
"""
import collections
from bfxapi.models.order_book import OrderBook
import os
import sys
# from datetime import datetime
# from pytz import utc

from bfxapi import Client
# from bfxapi.websockets.bfx_websocket import Flags
import kafka

# import time

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
producer = kafka.KafkaProducer(bootstrap_servers=host)
kafka.KafkaClient(bootstrap_servers=host).add_topic('all')

pairs = ["ABSUSD", "AGIUSD", "AIDUSD", "AIOBTC", "AIOUSD", "ALGBTC", "ALGUSD", "ALGUST", "AMPBTC", "AMPUSD", "AMPUST",
         "ANTBTC", "ANTETH", "ANTUSD", "ASTUSD", "ATMUSD", "ATOBTC", "ATOETH", "ATOUSD", "AUCUSD", "AVTUSD", "BABBTC",
         "BABUSD", "BABUST", "BATBTC", "BATETH", "BATUSD", "BFTBTC", "BFTUSD", "BNTUSD", "BOXUSD", "BSVBTC", "BSVUSD",
         "BTC:CNHT", "BTCEUR", "BTCGBP", "BTCJPY", "BTCUSD", "BTCUST", "BTCXCH", "BTGBTC", "BTGUSD", "BTTBTC", "BTTUSD",
         "CBTUSD", "CHZUSD", "CHZUST", "CLOBTC", "CLOUSD", "CNDETH", "CNDUSD", "CNH:CNHT", "CNNUSD", "CSXUSD", "CTXUSD",
         "DAIBTC", "DAIETH", "DAIUSD", "DATBTC", "DATETH", "DATUSD", "DGBBTC", "DGBUSD", "DGXETH", "DGXUSD", "DRNUSD",
         "DSHBTC", "DSHUSD", "DTAUSD", "DTHUSD", "DTXUSD", "DUSK:BTC", "DUSK:USD", "EDOBTC", "EDOETH", "EDOUSD",
         "ELFUSD", "ENJUSD", "EOSBTC", "EOSETH", "EOSEUR", "EOSGBP", "EOSJPY", "EOSUSD", "EOSUST", "ESSUSD", "ETCBTC",
         "ETCUSD", "ETHBTC", "ETHEUR", "ETHGBP", "ETHJPY", "ETHUSD", "ETHUST", "ETPBTC", "ETPETH", "ETPUSD", "EUSUSD",
         "EUTEUR", "EUTUSD", "EVTUSD", "FOAUSD", "FSNUSD", "FTTUSD", "FTTUST", "FUNETH", "FUNUSD", "GENUSD", "GNOUSD",
         "GNTBTC", "GNTETH", "GNTUSD", "GOTETH", "GOTEUR", "GOTUSD", "GSDUSD", "GTXUSD", "GTXUST", "HOTUSD", "IMPUSD",
         "INTUSD", "IOSBTC", "IOSETH", "IOSUSD", "IOTBTC", "IOTETH", "IOTEUR", "IOTGBP", "IOTJPY", "IOTUSD", "IQXEOS",
         "IQXUSD", "KANUSD", "KANUST", "KNCBTC", "KNCUSD", "LEOBTC", "LEOEOS", "LEOETH", "LEOUSD", "LEOUST", "LOOUSD",
         "LRCBTC", "LRCUSD", "LTCBTC", "LTCUSD", "LTCUST", "LYMUSD", "MANUSD", "MGOUSD", "MITUSD", "MKRBTC", "MKRDAI",
         "MKRETH", "MKRUSD", "MLNUSD", "MNABTC", "MNAUSD", "MTNUSD", "NCAUSD", "NECBTC", "NECETH", "NECUSD", "NEOBTC",
         "NEOETH", "NEOEUR", "NEOGBP", "NEOJPY", "NEOUSD", "ODEBTC", "ODEETH", "ODEUSD", "OKBBTC", "OKBUSD", "OKBUST",
         "OMGBTC", "OMGETH", "OMGUSD", "OMNBTC", "OMNUSD", "ONLUSD", "ORSUSD", "PAIUSD", "PASUSD", "PAXUSD", "PAXUST",
         "PNKETH", "PNKUSD", "POAUSD", "POYUSD", "QSHUSD", "QTMBTC", "QTMUSD", "RBTBTC", "RBTUSD", "RCNUSD", "RDNUSD",
         "REPBTC", "REPETH", "REPUSD", "REQUSD", "RIFBTC", "RIFUSD", "RLCBTC", "RLCUSD", "RRBUSD", "RRBUST", "RRTUSD",
         "RTEUSD", "SANBTC", "SANETH", "SANUSD", "SCRUSD", "SEEUSD", "SNGUSD", "SNTBTC", "SNTETH", "SNTUSD", "SPKUSD",
         "STJUSD", "SWMUSD", "TKNUSD", "TNBBTC", "TNBETH", "TNBUSD", "TRIUSD", "TRXBTC", "TRXETH", "TRXEUR", "TRXGBP",
         "TRXJPY", "TRXUSD", "TSDUSD", "TSDUST", "UDCUSD", "UDCUST", "UFRUSD", "UOSBTC", "UOSUSD", "USKBTC", "USKEOS",
         "USKETH", "USKUSD", "USKUST", "UST:CNHT", "USTUSD", "UTKUSD", "UTNUSD", "VEEUSD", "VETBTC", "VETUSD", "VLDUSD",
         "VSYBTC", "VSYUSD", "WAXBTC", "WAXUSD", "WBTETH", "WBTUSD", "WLOUSD", "WPRUSD", "WTCUSD", "XAUT:BTC",
         "XAUT:USD", "XAUT:UST", "XCHETH", "XCHUSD", "XLMBTC", "XLMETH", "XLMEUR", "XLMGBP", "XLMUSD", "XMRBTC",
         "XMRUSD", "XRAUSD", "XRPBTC", "XRPUSD", "XTZBTC", "XTZUSD", "XVGBTC", "XVGUSD", "YGGUSD", "YYWUSD", "ZBTUSD",
         "ZCNUSD", "ZECBTC", "ZECUSD", "ZILUSD", "ZRXBTC", "ZRXETH", "ZRXUSD"]
pairs = list(map(lambda x: 't' + x, pairs))
local_book = collections.defaultdict(OrderBook)
stats = collections.defaultdict(list)


# key -1 indicates ask, 1 indicates bid
# total_amount = {-1: 0, 1: 0}

# running_data = [running_total, running_squared_total, running_count] (of new orders)
# running_data = [0, 0, -1]

@bfx.ws.on('error')
def log_error(err):
    print("Error: {}".format(err))


def kafka_send(symbol, data):
    k = ",".join([exchange, symbol]).encode()
    v = ",".join(map(str, data)).encode()
    print(v)
    producer.send('all', key=k, value=v)


def update_order(data):
    # delta = [price, count, quantity, (bid/ask) flag, best_bid, best_ask, total_bid_amount, total_ask_amount, avg, var]
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
    delta = [p, count, q, flag, book.bids[0][0], book.asks[0][0], total_amount[1], total_amount[-1], 0, 0]
    for index, info in enumerate(side):
        if info[0] == p:

            delta[1] -= info[1]
            if count == 0:
                delta[2] = -info[2]
                total_amount[flag] += delta[2]
                del side[index]
                return delta
                # if change_count!=0:
            #     print("update with price", data, info)
            # if change_q!=0:
            #     print("update with quantity", data, info)
            # if data['data'][1]>count:
            #     if q>0:
            #         print("bid price increase", data, info[1], count)
            #     else:
            #         print("ask price increase", data, info[1], count)
            # else:
            #     if q>0:
            #         print("bid price decrease", data, info[1], count)
            #     else:
            #         print("ask price decrease", data, info[1], count)
            del side[index]
            delta[2] -= info[2]
            # print(side)
            break
    # if price level not present in the order book
    if count == 0:
        return []
    total_amount[flag] += delta[2]
    if delta[2] > 0:
        # avg
        delta[8] = running_data[0] / running_data[2]
        # var
        delta[9] = running_data[1] / running_data[2] - delta[9] ** 2
        running_data[2] += 1
        running_data[0] += delta[2]
        running_data[1] += delta[2] ** 2
    side.append([p, count, q])
    side.sort(key=lambda x: x[0], reverse=not flag < 0)
    return delta


@bfx.ws.on('order_book_update')
def log_update(data):
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
    if order_change:
        kafka_send(data['symbol'], order_change)
    # k = data['symbol'].encode()
    # v = ",".join(map(str, data['data'])).encode()
    # producer.send('all', key=k, value=v)


@bfx.ws.on('order_book_snapshot')
def log_snapshot(data):
    ob = local_book[data['symbol']] = OrderBook()
    total_amount = {}
    running_data = [0, 0, -1]
    stats[data['symbol']] = [total_amount, running_data]

    for x in bfx.ws.orderBooks[data['symbol']].bids:
        print(x[0], x[1])
        break
    ob.bids = [x[0].copy() for x in bfx.ws.orderBooks[data['symbol']].bids]
    ob.asks = [[x[0][0], x[0][1], -x[0][2]] for x in bfx.ws.orderBooks[data['symbol']].asks]
    ob.bids.sort(key=lambda x: x[0], reverse=True)
    ob.asks.sort(key=lambda x: x[0])
    for x in ob.bids:
        print(x[2])
        break
    print(ob.bids[0], ob.bids[0][0], ob.bids[0][1], ob.bids[0][2])
    total_amount[1] = sum(x[2] for x in ob.bids)
    total_amount[-1] = sum(x[2] for x in ob.asks)
    running_data[0] += sum(total_amount.values())
    running_data[1] += sum(x[2] ** 2 for x in ob.bids) + sum(x[2] ** 2 for x in ob.asks)
    running_data[2] += sum(x[1] for x in ob.bids) + sum(x[1] for x in ob.asks)
    print('initial_book')
    # print("Initial book: {}".format(data))
    # print(data['symbol'], bfx.ws.orderBooks[data['symbol']].asks)


async def start():
    # print(bfx.ws.orderBooks['tBTCUSD'].asks)
    # await bfx.ws.enable_flag(Flags.TIMESTAMP)
    await bfx.ws.subscribe('book', 'tBTCUSD', prec='P0', len='25')
    # await bfx.ws.subscribe('book', 'tALGUSD', len='100')
    # for pair in pairs:
    #     await bfx.ws.subscribe('book', pair, prec='P0', len='100')
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
