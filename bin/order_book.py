import collections

from sortedcontainers import SortedDict


def kafka_send(producer, topic, exchange, symbol, data, timestamp=None):
    k = ",".join([exchange, symbol]).encode()
    v = ",".join(map(str, data)).encode()
    producer.send(topic, key=k, value=v, timestamp_ms=timestamp)


class OrderBook:
    def __init__(self):

        self.bids = SortedDict()
        self.asks = SortedDict()
        self.stats = collections.defaultdict(list)

    def initialize_book(self, exchange, bids, asks):
        if exchange == 'bitfinex':
            return self.initializeBookForBitfinex(bids, asks)
        if exchange == 'binance':
            return self.initializeBookForBinance(bids, asks)

    def initializeBookForBitfinex(self, bids, asks):

        # para bids,asks are lists of tuples (x,y) where x=[price, count, quantity]
        self.bids = SortedDict({x[0][0]: [x[0][1], x[0][2]] for x in bids})
        self.asks = SortedDict({x[0][0]: [x[0][1], -x[0][2]] for x in asks})
        self._get_stats()

    def initializeBookForBinance(self, bids, asks):

        # para bids, asks are lists of [price : String, quantity: String]
        self.bids = SortedDict({float(x[0]): [1, float(x[1])] for x in bids})
        self.asks = SortedDict({float(x[0]): [1, float(x[1])] for x in asks})
        self._get_stats()

    def _get_stats(self):
        total_amount = {}
        running_data = [0, 0, 0]
        self.stats = [total_amount, running_data]

        total_amount[1] = sum(x[1] for x in self.bids.values())
        total_amount[-1] = sum(x[1] for x in self.asks.values())
        running_data[0] += sum(total_amount.values())
        running_data[1] += sum(x[1] ** 2 for x in self.bids.values()) + sum(x[1] ** 2 for x in self.asks.values())
        running_data[2] += sum(x[0] for x in self.bids.values()) + sum(x[0] for x in self.asks.values())

    def update_order(self, data):
        # delta = [price, count, quantity, (bid/ask) flag, best_bid, best_ask, total_bid_amount, total_ask_amount,
        # avg, var]

        p, count, q = data
        book = self
        total_amount = book.stats[0]
        running_data = book.stats[1]

        if q < 0:
            flag = -1
            side = book.asks
            q = -q
        else:
            flag = 1
            side = book.bids
        best_bid = book.bids.peekitem()[0] if book.bids else 0
        best_ask = book.asks.peekitem(0)[0] if book.asks else float("inf")
        delta = [p, count, q, flag, best_bid, best_ask, total_amount[1], total_amount[-1], 0, 0]
        if p in side:
            delta[1] -= side[p][0]
            delta[2] -= side[p][1]
            if count == 0:
                total_amount[flag] += delta[2]
                del side[p]
                return delta
            side[p][0] = count
            side[p][1] = q
        else:
            return []
        if delta[2] == 0:
            return []

        total_amount[flag] += delta[2]
        # avg
        delta[8] = running_data[0] / running_data[2]
        # var
        delta[9] = running_data[1] / running_data[2] - delta[8] ** 2

        # update running data
        if delta[2] > 0:
            running_data[2] += 1
            running_data[0] += delta[2]
            running_data[1] += delta[2] ** 2
            
        return delta
