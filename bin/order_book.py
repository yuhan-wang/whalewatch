import collections


def kafka_send(producer, topic, exchange, symbol, data, timestamp=None):
    k = ",".join([exchange, symbol]).encode()
    v = ",".join(map(str, data)).encode()
    producer.send(topic, key=k, value=v, timestamp_ms=timestamp)


class OrderBook:
    def __init__(self):

        self.bids = []
        self.asks = []
        self.stats = collections.defaultdict(list)

    def initialize_book(self, exchange, bids, asks):
        if exchange == 'bitfinex':
            return self.initializeBookForBitfinex(bids, asks)
        if exchange == 'binance':
            return self.initializeBookForBinance(bids, asks)

    def initializeBookForBitfinex(self, bids, asks):

        # para bids,asks are lists of tuples (x,y) where x=[price, count, quantity]
        self.bids = [x[0].copy() for x in bids]
        self.asks = [[x[0][0], x[0][1], -x[0][2]] for x in asks]
        self._get_stats()

    def initializeBookForBinance(self, bids, asks):

        # para bids, asks are lists of [price : String, quantity: String]
        self.bids = [[float(x[0]), 1, float(x[1])] for x in bids]
        self.asks = [[float(x[0]), 1, float(x[1])] for x in asks]
        self._get_stats()

    def _get_stats(self):
        self.bids.sort(key=lambda x: x[0], reverse=True)
        self.asks.sort(key=lambda x: x[0])

        # key -1 indicates ask, 1 indicates bid total_amount = {-1: 0, 1: 0}
        # running_data = [running_total, running_squared_total, running_count] (of new orders)
        total_amount = {}
        running_data = [0, 0, 0]
        self.stats = [total_amount, running_data]

        total_amount[1] = sum(x[2] for x in self.bids)
        total_amount[-1] = sum(x[2] for x in self.asks)
        running_data[0] += sum(total_amount.values())
        running_data[1] += sum(x[2] ** 2 for x in self.bids) + sum(x[2] ** 2 for x in self.asks)
        running_data[2] += sum(x[1] for x in self.bids) + sum(x[1] for x in self.asks)

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
                    return delta
                del side[index]
                delta[2] -= info[2]
                break

        # if price level not present in the order book or no change at all
        if count == 0 or delta[2] == 0:
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

        side.append([p, count, q])
        side.sort(key=lambda x: x[0], reverse=not flag < 0)
        return delta
