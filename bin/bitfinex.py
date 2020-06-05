
"""

Need to implement own code to get new order or manage an order book in spark
"""
import os
import sys
from datetime import datetime
from pytz import utc

from bfxapi import Client
import kafka
import time

producer_timings = {}
def calculate_thoughput(timing, n_messages=1000000, msg_size=100):
    print("Processed {0} messsages in {1:.2f} seconds".format(n_messages, timing))
    print("{0:.2f} MB/s".format((msg_size * n_messages) / timing / (1024*1024)))
    print("{0:.2f} Msgs/s".format(n_messages / timing))

bfx = Client()
host = ['localhost:9092']
producer = kafka.KafkaProducer(bootstrap_servers=host)
kafka.KafkaClient(bootstrap_servers=host).add_topic('all')

pairs=["ABSUSD","AGIUSD","AIDUSD","AIOBTC","AIOUSD","ALGBTC","ALGUSD","ALGUST","AMPBTC","AMPUSD","AMPUST","ANTBTC","ANTETH","ANTUSD","ASTUSD","ATMUSD","ATOBTC","ATOETH","ATOUSD","AUCUSD","AVTUSD","BABBTC","BABUSD","BABUST","BATBTC","BATETH","BATUSD","BFTBTC","BFTUSD","BNTUSD","BOXUSD","BSVBTC","BSVUSD","BTC:CNHT","BTCEUR","BTCGBP","BTCJPY","BTCUSD","BTCUST","BTCXCH","BTGBTC","BTGUSD","BTTBTC","BTTUSD","CBTUSD","CHZUSD","CHZUST","CLOBTC","CLOUSD","CNDETH","CNDUSD","CNH:CNHT","CNNUSD","CSXUSD","CTXUSD","DAIBTC","DAIETH","DAIUSD","DATBTC","DATETH","DATUSD","DGBBTC","DGBUSD","DGXETH","DGXUSD","DRNUSD","DSHBTC","DSHUSD","DTAUSD","DTHUSD","DTXUSD","DUSK:BTC","DUSK:USD","EDOBTC","EDOETH","EDOUSD","ELFUSD","ENJUSD","EOSBTC","EOSETH","EOSEUR","EOSGBP","EOSJPY","EOSUSD","EOSUST","ESSUSD","ETCBTC","ETCUSD","ETHBTC","ETHEUR","ETHGBP","ETHJPY","ETHUSD","ETHUST","ETPBTC","ETPETH","ETPUSD","EUSUSD","EUTEUR","EUTUSD","EVTUSD","FOAUSD","FSNUSD","FTTUSD","FTTUST","FUNETH","FUNUSD","GENUSD","GNOUSD","GNTBTC","GNTETH","GNTUSD","GOTETH","GOTEUR","GOTUSD","GSDUSD","GTXUSD","GTXUST","HOTUSD","IMPUSD","INTUSD","IOSBTC","IOSETH","IOSUSD","IOTBTC","IOTETH","IOTEUR","IOTGBP","IOTJPY","IOTUSD","IQXEOS","IQXUSD","KANUSD","KANUST","KNCBTC","KNCUSD","LEOBTC","LEOEOS","LEOETH","LEOUSD","LEOUST","LOOUSD","LRCBTC","LRCUSD","LTCBTC","LTCUSD","LTCUST","LYMUSD","MANUSD","MGOUSD","MITUSD","MKRBTC","MKRDAI","MKRETH","MKRUSD","MLNUSD","MNABTC","MNAUSD","MTNUSD","NCAUSD","NECBTC","NECETH","NECUSD","NEOBTC","NEOETH","NEOEUR","NEOGBP","NEOJPY","NEOUSD","ODEBTC","ODEETH","ODEUSD","OKBBTC","OKBUSD","OKBUST","OMGBTC","OMGETH","OMGUSD","OMNBTC","OMNUSD","ONLUSD","ORSUSD","PAIUSD","PASUSD","PAXUSD","PAXUST","PNKETH","PNKUSD","POAUSD","POYUSD","QSHUSD","QTMBTC","QTMUSD","RBTBTC","RBTUSD","RCNUSD","RDNUSD","REPBTC","REPETH","REPUSD","REQUSD","RIFBTC","RIFUSD","RLCBTC","RLCUSD","RRBUSD","RRBUST","RRTUSD","RTEUSD","SANBTC","SANETH","SANUSD","SCRUSD","SEEUSD","SNGUSD","SNTBTC","SNTETH","SNTUSD","SPKUSD","STJUSD","SWMUSD","TKNUSD","TNBBTC","TNBETH","TNBUSD","TRIUSD","TRXBTC","TRXETH","TRXEUR","TRXGBP","TRXJPY","TRXUSD","TSDUSD","TSDUST","UDCUSD","UDCUST","UFRUSD","UOSBTC","UOSUSD","USKBTC","USKEOS","USKETH","USKUSD","USKUST","UST:CNHT","USTUSD","UTKUSD","UTNUSD","VEEUSD","VETBTC","VETUSD","VLDUSD","VSYBTC","VSYUSD","WAXBTC","WAXUSD","WBTETH","WBTUSD","WLOUSD","WPRUSD","WTCUSD","XAUT:BTC","XAUT:USD","XAUT:UST","XCHETH","XCHUSD","XLMBTC","XLMETH","XLMEUR","XLMGBP","XLMUSD","XMRBTC","XMRUSD","XRAUSD","XRPBTC","XRPUSD","XTZBTC","XTZUSD","XVGBTC","XVGUSD","YGGUSD","YYWUSD","ZBTUSD","ZCNUSD","ZECBTC","ZECUSD","ZILUSD","ZRXBTC","ZRXETH","ZRXUSD"]
pairs=list(map(lambda x:'t'+x, pairs))
@bfx.ws.on('error')
def log_error(err):
    print("Error: {}".format(err))

@bfx.ws.on('order_book_update')
def log_update(data):
    #print(bfx.ws.orderBooks[data['symbol']].asks)
    #print(bfx.ws.orderBooks['tBTCUSD'].asks)
    print("Book update: {}".format(data))
    #print(type(data), sys.getsizeof(data)+sys.getsizeof(data['symbol'])+sys.getsizeof(data['data']))
    k=data['symbol'].encode()
    oid, p, q= data['data']
    if q<0:
        side=bfx.ws.orderBooks[data['symbol']].asks
    else:
        side=bfx.ws.orderBooks[data['symbol']].bids
    print(bfx.ws.orderBooks[data['symbol']].asks)
    v=",".join(map(str, data['data'])).encode()
    producer.send('all', key=k, value=v)

@bfx.ws.on('order_book_snapshot')
def log_snapshot(data):
    print ("Initial book: {}".format(data))
    print(bfx.ws.orderBooks[data['symbol']].asks)


async def start():
    #print(bfx.ws.orderBooks['tBTCUSD'].asks)
    await bfx.ws.subscribe('book', 'tALGUSD', prec='R0', len='100')
    #await bfx.ws.subscribe('book', 'tALGUSD', len='100')
    #for pair in pairs:
        #await bfx.ws.subscribe('book', pair, prec='R0')



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

