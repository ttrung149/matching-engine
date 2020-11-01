import heapq

#==============================================================================
# Order class
#==============================================================================
class Order:
    def __init__(self, processed_order, timestamp, order_id, side, price, size):
        self.processed_order = processed_order
        self.time = timestamp
        self.id = order_id
        self.side = side
        self.price = price
        self.size = size

    def __lt__(self, other):
        if self.side == 'buy':
            if (self.price > other.price): return True
            if (self.price == other.price and self.time < other.time): return True
            if (self.price == other.price and self.time == other.time):
                return self.processed_order < other.processed_order
            return False

        else:
            if (self.price < other.price): return True
            if (self.price == other.price and self.time < other.time): return True
            if (self.price == other.price and self.time == other.time):
                return self.processed_order < other.processed_order
            return False

    def __repr__(self):
        return 'Order(time:{}, id:{}, side:{}, price:{}, size:{})'.format(
            self.time, self.id, self.side, self.price, self.size
        )

#==============================================================================
# Aggregated Book - Indexes size of order by price level
#==============================================================================
class AggregatedBook:
    def __init__(self):
        self.book = {}

    def __getitem__(self, price):
        return self.book[price]

    def add_order(self, price, size):
        if price not in self.book:
            self.book[price] = size
        else:
            self.book[price] += size

    def exec_order(self, price, size):
        if price not in self.book: return

        self.book[price] -= size
        if self.book[price] == 0: self.book.pop(price)

#==============================================================================
# Matching Engine class - Maintain order books and trade execs
#==============================================================================
class MatchingEngine:
    def __init__(self, nbbo_file, trades_file):
        self.nbbo_file = nbbo_file
        self.trades_file = trades_file
        self.bids = []
        self.asks = []
        self.bids_aggregated_book = AggregatedBook()
        self.asks_aggregated_book = AggregatedBook()
        self.valid_orders_table = {}

    def parse_order(self, processed_order, line):
        cols = line.split(',')
        _id = int(cols[2])

        # Handle order insertion
        if cols[1] == 'insert':
            _side, _price, _size = cols[3], int(cols[4]), int(cols[5])
            self.valid_orders_table[_id] = (_side, _price, _size)

            if _side == 'buy':
                self.bids_aggregated_book.add_order(_price, _size)
            elif _side == 'sell':
                self.asks_aggregated_book.add_order(_price, _size)

            return Order(processed_order, int(cols[0]), _id, cols[3], _price, _size)

        # Handle order cancellation
        elif cols[1] == 'cancel':
            if _id not in self.valid_orders_table:
                return None
            _side, _price, _size = self.valid_orders_table.pop(_id)

            if _side == 'buy':
                self.bids_aggregated_book.exec_order(_price, _size)
            elif _side == 'sell':
                self.asks_aggregated_book.exec_order(_price, _size)

        return None

    def log_nbbo(self):
        while len(self.bids) > 0 and self.bids[0].id not in self.valid_orders_table:
            heapq.heappop(self.bids)
        while len(self.asks) > 0 and self.asks[0].id not in self.valid_orders_table:
            heapq.heappop(self.asks)

        bid_price = 0 if len(self.bids) == 0 else self.bids[0].price
        bid_size  = 0 if len(self.bids) == 0 else self.bids_aggregated_book[bid_price]
        ask_price = 0 if len(self.asks) == 0 else self.asks[0].price
        ask_size  = 0 if len(self.asks) == 0 else self.asks_aggregated_book[ask_price]

        self.nbbo_file.write(
            "{},{},{},{}\n".format(bid_price, bid_size, ask_price, ask_size)
        )

    def log_trades(self, price, size, buy_id, sell_id):
        self.trades_file.write(
            "{},{},{},{}\n".format(price, size, buy_id, sell_id)
        )

    def exec_buy_order(self, order):
        # Execute crossed buy order
        while len(self.asks) > 0 and order.price >= self.asks[0].price:
            top = self.asks[0]
            if top.id not in self.valid_orders_table:
                heapq.heappop(self.asks)
                continue

            # Case 1: Best ask cannot fulfill buy order
            if order.size > top.size:
                order.size -= top.size

                self.valid_orders_table.pop(top.id, None)
                self.bids_aggregated_book.exec_order(order.price, top.size)
                self.asks_aggregated_book.exec_order(top.price, top.size)
                self.log_trades(top.price, top.size, order.id, top.id)
                heapq.heappop(self.asks)

            # Case 2: Buy order is executed within the best ask
            elif order.size < top.size:
                top.size -= order.size

                self.valid_orders_table.pop(order.id, None)
                self.bids_aggregated_book.exec_order(order.price, order.size)
                self.asks_aggregated_book.exec_order(top.price, order.size)
                self.log_trades(top.price, order.size, order.id, top.id)
                self.log_nbbo()
                return

            # Case 3: Buy order matches the best ask
            else:
                self.valid_orders_table.pop(order.id, None)
                self.valid_orders_table.pop(top.id, None)
                self.bids_aggregated_book.exec_order(order.price, order.size)
                self.asks_aggregated_book.exec_order(top.price, top.size)
                self.log_trades(top.price, order.size, order.id, top.id)
                self.log_nbbo()
                return

        # Add buy order to outstanding bids
        heapq.heappush(self.bids, order)
        self.log_nbbo()

    def exec_sell_order(self, order):
        # Execute crossed sell order
        while len(self.bids) > 0 and order.price <= self.bids[0].price:
            top = self.bids[0]
            if top.id not in self.valid_orders_table:
                heapq.heappop(self.bids)
                continue

            # Case 1: Best bid cannot fulfill sell order
            if order.size > top.size:
                order.size -= top.size

                self.valid_orders_table.pop(top.id, None)
                self.bids_aggregated_book.exec_order(top.price, top.size)
                self.asks_aggregated_book.exec_order(order.price, top.size)
                self.log_trades(order.price, top.size, top.id, order.id)
                heapq.heappop(self.bids)

            # Case 2: Sell order is executed within the best bid
            elif order.size < top.size:
                top.size -= order.size

                self.valid_orders_table.pop(order.id, None)
                self.bids_aggregated_book.exec_order(top.price, order.size)
                self.asks_aggregated_book.exec_order(order.price, order.size)
                self.log_trades(order.price, order.size, top.id, order.id)
                self.log_nbbo()
                return

            # Case 3: Sell order matches the best bid
            else:
                self.valid_orders_table.pop(order.id, None)
                self.valid_orders_table.pop(top.id, None)
                self.bids_aggregated_book.exec_order(top.price, top.size)
                self.asks_aggregated_book.exec_order(order.price, order.size)
                self.log_trades(order.price, order.size, top.id, order.id)
                self.log_nbbo()
                return

        # Add sell order to outstanding asks
        heapq.heappush(self.asks, order)
        self.log_nbbo()

#==============================================================================
# Main run function
#==============================================================================
def run(input_path):
    log_bbos_path = 'output_bbos.csv'
    log_trades_path = 'output_trades.csv'

    with open(input_path, 'r') as input_file, \
         open(log_bbos_path, 'w') as nbbo_file, \
         open(log_trades_path, 'w') as trades_file:

        nbbo_file.write('bid_price,bid_size,ask_price,ask_size\n')
        trades_file.write('trade_price,trade_size,buy_order_id,sell_order_id\n')

        next(input_file)
        m = MatchingEngine(nbbo_file, trades_file)

        processed_order = 0
        for line in input_file:
            order = m.parse_order(processed_order, line)
            processed_order += 1

            if order != None:
                if order.side == 'buy':     m.exec_buy_order(order)
                elif order.side == 'sell':  m.exec_sell_order(order)
            else:
                m.log_nbbo()

    return log_bbos_path, log_trades_path