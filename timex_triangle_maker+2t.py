import collections
import datetime
import logging
import configparser
import sys
import uuid
import ccxt
import time
import random
import telebot
from DataBase import triangle_database
import random
import string
import re
import libtmux

import py_timex.client as timex


cp = configparser.ConfigParser()

server = libtmux.Server()
if len(sys.argv) != 2:
    # print("Usage %s <config.ini>" % sys.argv[0])
    sys.exit(1)
cp.read(sys.argv[1], "utf-8")

FORMAT = '%(asctime)s %(levelname)s %(filename)s:%(lineno)d %(message)s (%(funcName)s)'
logging.basicConfig(format=FORMAT)
log = logging.getLogger("sample bot")
log.setLevel(logging.DEBUG)

class TriangleBot:
    _raw_updates = 0
    _group_updates = 0

    chat_id = cp["TELEGRAM"]["chat_id"]
    telegram_bot = telebot.TeleBot(cp["TELEGRAM"]["token"])
    bot = ccxt.timex({})
    markets = bot.load_markets()
    dataBase = triangle_database(telegram_bot, chat_id)

    pairs = set(markets)
    pairs.discard('TIMEV1/BTC')
    pairs.discard('TIMEV1/ETH')
    splited_pairs = {}
    triangles_coins = []
    coins = []

    profit = 0.001
    fee = 0.001
    changes = {'USD': 1, 'USDT': 1, 'USDC': 1}
    amounts_session_start = None
    amounts_total_start = None

    start_time = time.time()

    def __init__(self, client: timex.WsClientTimex):
        self.session_id = self.id_generator(size=6)
        self._my_orders = dict[str: timex.Order]()
        self._client = client
        self.depth = 3
        self.existing_triangles = {}
        client.on_first_connect = self.on_first_connect
        client.subscribe_balances(self.handle_balance)
        client.subscribe_orders(self.handle_order)
        # client.subscribe_group_order_book(timex.ETHAUDT, self.handle_group_order_book_update)
        for pair in self.pairs:
            self.splited_pairs.update({pair: pair.split('/')[0] + pair.split('/')[1]})
            client.subscribe_raw_order_book(self.splited_pairs[pair], self.handle_raw_order_book_update)


    def id_generator(self, size, chars=string.ascii_uppercase + string.digits):
        return ''.join(random.choice(chars) for _ in range(size))

    # def handle_group_order_book_update(self, ob: timex.OrderBook):
    #     self._group_updates += 1
    #     # log.info("group: updates: %d, asks: %d, bids: %d",
    #     self._group_updates,
    #     #          len(ob.asks),
    #     #          len(ob.bids),
    #     #          )
    #     print(ob)


    def handle_raw_order_book_update(self, ob: timex.OrderBook):
        self._raw_updates += 1
        # log.info("raw: updates: %d, asks: %d, bids: %d",
        # self._group_updates,
        #          len(ob.asks),
        #          len(ob.bids),
        #          )
        # print(ob)
        # for orderbook in self._client.raw_order_books.values():
        #     print(f"Market: {orderbook.market} Len asks: {len(orderbook.asks)} Len bids: {len(orderbook.bids)}")
        if self._raw_updates == 198:
            self.define_coins()
            self.changes_defining()
            self.dataBase.sql_balances_update(self._client.balances, self.changes, self.session_id)
            self.find_all_triangles()
            self.check_balance()

        # if not self._raw_updates % 200:
            # print('OPENED ORDERS:')
            # print("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
            # print(self._my_orders)
            # print()
            # print()

        if self._raw_updates > 199:
            if not self._raw_updates % 5000:
                self.check_balance()
                if not set(self._my_orders) == set(self.existing_triangles):
                    self._my_orders = {}
                    self.existing_triangles = {}
            if set(self._my_orders) == set(self.existing_triangles):
                if not self._raw_updates % 50:
                    self.change_existing_orders()
            # if not self._raw_updates % 200:
            #     print(self._my_orders)
                # print(f"Triangle count ended")
            # print(f"Total updates: {self._raw_updates}\nTime since started: {time.time() - self.start_time}")


    def on_first_connect(self):
        self._client.create_orders([
            timex.NewOrder(
                price=1.1,
                quantity=36.6,
                side=timex.ORDER_SIDE_BUY,
                type=timex.ORDER_TYPE_LIMIT,
                symbol=timex.ETHAUDT,
                expire_in_seconds=3,
                client_order_id='START',
            )], self.handle_create_orders)


    def handle_create_orders(self, obj):
        if obj.get('responseBody'):
            if obj['responseBody'].get('orders'):
                for new_order in obj['responseBody']['orders']:
                    client_id = new_order['clientOrderId']
                    # print(f"Created order: {client_id}")
                    self._my_orders[client_id] = new_order
        # log.info(obj)
        pass


    def handle_balance(self, balance: timex.Balance):
        # self.sorting_triangles()
        # log.info(balance)
        pass

    def find_triangle(self, order):
        if self.existing_triangles.get(order.client_order_id):
            balancing_triangle = self.existing_triangles[order.client_order_id]
            self.existing_triangles.pop(order.client_order_id)
            return balancing_triangle
        return None

    def handle_order(self, order: timex.Order):
        # print(f"ORDER UPDATE. Line 145:")
        # print({order['client_order_id: order})
        # print()
        # log.info(order)
        if order.status == 'FILLED':
            self._my_orders.pop(order.client_order_id)
            triangle = self.find_triangle(order)
            if triangle:
                orders = [self.create_order_data(triangle[1]),
                          self.create_order_data(triangle[2])]
                self._client.create_orders(orders, self.handle_create_orders)
                # print(f"Order executed:\n{triangle}")
                self.to_base_data(triangle)
                self.telegram_bot.send_message(self.chat_id, str(triangle))
            else:
                self.telegram_bot.send_message(self.chat_id, f'ORDER EXECUTED. TRIANGLE NOT FOUND.\n{order}')
            self.sorting_triangles()
            return
        if order.status == 'PARTIAL':
            self._client.delete_orders([order.id], self.handle_delete_orders)
            self._my_orders.pop(order.client_order_id)
            triangle = self.find_triangle(order)
            if triangle:
                kef = float(order.filled_quantity) / (triangle[0]['start_amount'])
                orders = [self.create_order_data(triangle[1], kef),
                          self.create_order_data(triangle[2], kef)]
                self._client.create_orders(orders, self.handle_create_orders)
                self.to_base_data(triangle, kef)
                self.telegram_bot.send_message(self.chat_id, f'Partial: {kef * 100}%\n' + str(triangle))
            else:
                self.telegram_bot.send_message(self.chat_id, f'ORDER EXECUTED. TRIANGLE NOT FOUND.\n{order}')
            self.sorting_triangles()
            return
        if order.client_order_id == 'START':
            self._client.delete_orders([order.id], self.handle_delete_orders)


    def handle_delete_orders(self, obj):
        if obj['responseBody'].get('changedOrders'):
            for cancelled_order in obj['responseBody']['changedOrders']:
                client_id = cancelled_order['newOrder']['clientOrderId']
                if self.existing_triangles.get(client_id):
                    self.existing_triangles.pop(client_id)
                if self._my_orders.get(client_id):
                    self._my_orders.pop(client_id)
                # print(f"Deleted order: {client_id}")
                    # print(f"Order deleted from base:\n {cancelled_order}")
        # log.info(obj)


    def run(self):
        try:
            return self._client.run_updater()
        except KeyboardInterrupt:
            self._client.wait_closed()

    def to_base_data(self, triangle, kef = 1):
        profit = triangle[0]['profit_abs'] / triangle[0]['start_amount']
        profit_abs = triangle[0]['profit_abs'] * kef
        profit_usd = profit_abs * self.changes[triangle[0]['main_coin']]
        to_base = {"order_place_date": triangle[0]['timestamp'],
        "order_execute_date": time.time(),
        "triangle": f"{triangle[0]['pair']} {triangle[1]['pair']} {triangle[2]['pair']}",
        "maker_pair": triangle[0]['pair'],
        "maker_side": triangle[0]['side'],
        "execute_percent": kef * 100,
        "maker_order_price": triangle[0]['price'],
        "position_in_spread": triangle[0]['position'],
        "maker_coin_amount": triangle[0]['amount'],
        "taker_pair_1": triangle[1]['pair'],
        "taker_1_side": triangle[1]['side'],
        "taker_1_order_price": triangle[1]['price'],
        "taker_1_depth": triangle[1]['depth'],
        "taker_1_coin_amount": triangle[1]['amount'] * kef,
        "taker_pair_2": triangle[2]['pair'],
        "taker_2_side": triangle[2]['side'],
        "taker_2_order_price": triangle[2]['price'],
        "taker_2_depth": triangle[2]['depth'],
        "taker_2_coin_amount": triangle[2]['amount'] * kef,
        "deal_result_perc": profit,
        "deal_result_abs": profit_abs,
        "profit_coin": triangle[0]['main_coin'],
        "profit_USD": profit_usd,
        "order_hang_time": time.time() - triangle[0]['timestamp']
        }
        self.DataBase.base_update(to_base)

    def find_all_triangles(self):
        pairs = self.pairs
        orderbooks = self._client.raw_order_books
        triangle_sets = []
        triangles_coins = []
        for pair_1 in pairs:
            spltd_pair_1 = self.splited_pairs[pair_1]
            if not len(orderbooks[spltd_pair_1].bids) or not len(orderbooks[spltd_pair_1].asks):
                continue
            for pair_2 in pairs:
                spltd_pair_2 = self.splited_pairs[pair_2]
                if not len(orderbooks[spltd_pair_2].bids) or not len(orderbooks[spltd_pair_2].asks):
                    continue
                for pair_3 in pairs:
                    spltd_pair_3 = self.splited_pairs[pair_3]
                    if not len(orderbooks[spltd_pair_3].bids) or not len(orderbooks[spltd_pair_3].asks):
                        continue
                    if pair_1 == pair_2 or pair_1 == pair_3 or pair_2 == pair_3:
                        continue
                    all_coins = [pair_1.split('/')[0],
                                 pair_1.split('/')[1],
                                 pair_2.split('/')[0],
                                 pair_2.split('/')[1],
                                 pair_3.split('/')[0],
                                 pair_3.split('/')[1]]
                    if 'COMP' in all_coins:
                        continue
                    flag = False
                    for coin in all_coins:
                        if all_coins.count(coin) != 2:
                            flag = True
                    if flag:
                        flag = False
                        continue
                    if {pair_1, pair_2, pair_3} not in triangle_sets:
                        triangle_sets.append({pair_1, pair_2, pair_3})
                        triangles_coins.append({'coins': list(set(all_coins)),
                                                'pairs': [pair_1, pair_2, pair_3],
                                                'max_order_amount': 0})
        self.triangles_coins = triangles_coins
        self.define_pairs(triangle_sets)
        self.define_coins()
        self.sorting_triangles()


    def define_pairs(self, triangle_sets):
        pairs = []
        for triangle in triangle_sets:
            for pair in triangle:
                pairs.append(pair)
        self.pairs = set(pairs)


    def define_coins(self):
        balances = self._client.balances
        coins = []
        for balance in balances.values():
            if balance.total_balance != '0':
                coins.append(balance.currency)
        self.coins = coins


    def sorting_triangles(self):
        new_triangles_coins = []
        balance = self._client.balances
        for triangle in self.triangles_coins:
            for coin_1 in triangle['coins']:
                for pair_1 in triangle['pairs']:
                    for pair_2 in triangle['pairs']:
                        for pair_3 in triangle['pairs']:
                            if pair_1 == pair_2 or pair_1 == pair_3 or pair_2 == pair_3:
                                continue
                            if coin_1 in pair_1 and coin_1 in pair_3:
                                if coin_1 == pair_1.split('/')[0]:
                                    coin_2 = pair_1.split('/')[1]
                                else:
                                    coin_2 = pair_1.split('/')[0]
                                coin_3 = [x for x in triangle['coins'] if x not in [coin_1, coin_2]][0]
                                if balance[coin_1].total_balance != '0':
                                    if balance[coin_2].total_balance != '0':
                                        if balance[coin_3].total_balance != '0':
                                            max_order_amount = self.define_max_order_amount(coin_1, coin_2, coin_3)
                                            new_triangles_coins.append({'coins': [coin_1, coin_2, coin_3],
                                                                        'pairs': [pair_1, pair_2, pair_3],
                                                                        'max_order_amount': max_order_amount})
        self.triangles_coins = new_triangles_coins


    def define_max_order_amount(self, coin_1, coin_2, coin_3):
        balance = self._client.balances
        changes = self.changes
        coin1usdAmount = float(balance[coin_1].total_balance) * changes[coin_1]
        coin2usdAmount = float(balance[coin_2].total_balance) * changes[coin_2]
        coin3usdAmount = float(balance[coin_3].total_balance) * changes[coin_3]
        return min(coin1usdAmount, coin2usdAmount, coin3usdAmount) / 2


    def defining_middle_spread_price(self, orderbooks, pair):
        if len(orderbooks[pair].bids) and len(orderbooks[pair].asks):
            change = (orderbooks[pair].bids[0].price + orderbooks[pair].asks[0].price) / 2
            return change
        else:
            client = self._client
            client.subscribe_raw_order_book(pair, self.handle_raw_order_book_update)
            return None


    def finding_audt_change_price(self):
        orderbooks = self._client.raw_order_books
        btcusd_price = self.defining_middle_spread_price(orderbooks, 'BTCUSD')
        btcaudt_price = self.defining_middle_spread_price(orderbooks, 'BTCAUDT')
        ethusd_price = self.defining_middle_spread_price(orderbooks, 'ETHUSD')
        ethaudt_price = self.defining_middle_spread_price(orderbooks, 'ETHAUDT')
        if btcusd_price and btcaudt_price and ethusd_price and ethaudt_price:
            change_AUDT = round((ethusd_price / ethaudt_price + btcusd_price / btcaudt_price) / 2, 3)
        elif btcusd_price and btcaudt_price:
            change_AUDT = round(btcusd_price / btcaudt_price, 3)
        elif ethusd_price and ethaudt_price:
            change_AUDT = round(ethusd_price / ethaudt_price, 3)
        self.changes.update({'AUDT': change_AUDT})


    def changes_defining(self):
        orderbooks = self._client.raw_order_books
        self.finding_audt_change_price()
        for coin in self.coins:
            if 'USD' in coin:
                continue
            if coin == 'AUDT':
                pass
            for x in ['USDT', 'USD', 'USDC', 'AUDT']:
                try:
                    if x != 'AUDT':
                        change = self.defining_middle_spread_price(orderbooks, coin + x)
                        if change:
                            self.changes.update({coin: change})
                            break
                    else:
                        change = self.defining_middle_spread_price(orderbooks, coin + x)
                        if change:
                            change = change / self.changes['AUDT']
                            self.changes.update({coin: change})
                except:
                    pass


    def balancing_adopt(self, pair, side):
        balancing_orders = self.balancing()
        extra_liquidity = 0
        if len(balancing_orders):
            for order in balancing_orders:
                if side == order['side'] and pair == order['pair']:
                    extra_liquidity = order['amount']
        return extra_liquidity


    def defining_coins_chain(self, triangle):
        orderbooks = self._client.raw_order_books
        coin_1, coin_2, coin_3 = triangle['coins'][0], triangle['coins'][1], triangle['coins'][2]
        pair_1, pair_2, pair_3 = triangle['pairs'][0], triangle['pairs'][1], triangle['pairs'][2]
        coins_chain = {}

        # {'pair': balance_pair,
        #  'amount': balancing_amount,
        #  'side': side}
        # defining coins and pairs
        for num, coin, pair in [(1, coin_1, pair_1), (2, coin_2, pair_2), (3, coin_3, pair_3)]:
            if coin == pair.split('/')[0]:
                side = 'SELL'
                orderbook = orderbooks[self.splited_pairs[pair]].bids
            else:
                side = 'BUY'
                orderbook = orderbooks[self.splited_pairs[pair]].asks
            extra_liquidity = self.balancing_adopt(pair, side)
            coins_chain.update({'coin_' + str(num): {'coin': coin,
                                                     'pair': pair,
                                                     'side': side,
                                                     'orderbook': orderbook,
                                                     'extra': extra_liquidity}})
        return coins_chain

    #TESTS REQUIRED
    def block_liq_define(self, coins_chain):
        orders_2_pair = []
        orders_3_pair = []
        pair_2 = self.splited_pairs[coins_chain['coin_2']['pair']]
        pair_3 = self.splited_pairs[coins_chain['coin_3']['pair']]
        orders = self._my_orders
        for order in orders.values():
            if order['symbol'] == pair_2:
                orders_2_pair.append(order['price'])
            elif order['symbol'] == pair_3:
                orders_3_pair.append(order['price'])
        return orders_2_pair, orders_3_pair

    def defining_depth_counts(self, coins_chain):
        # print(self._my_orders)
        #     print(order_data)
        # counting into deep of liquidity
        orders_2_pair, orders_3_pair = self.block_liq_define(coins_chain)
        depth_count_2 = []
        depth_count_3 = []
        for position in range(self.depth):
            if len(coins_chain['coin_2']['orderbook']) < position + 1:
                break
            price2 = coins_chain['coin_2']['orderbook'][position].price
            if str(price2) in orders_2_pair:
                continue
            amount2 = sum([coins_chain['coin_2']['orderbook'][x].volume for x in range(position + 1)])
            usdAmount2 = amount2 * self.changes[coins_chain['coin_2']['pair'].split('/')[0]]
            depth_chain_2 = {'depth': position,
                             'price': price2,
                             'amount': amount2,
                             'usdAmount': usdAmount2}
            depth_count_2.append(depth_chain_2)
        for position in range(self.depth):
            if len(coins_chain['coin_3']['orderbook']) < position + 1:
                break
            price3 = coins_chain['coin_3']['orderbook'][position].price
            if str(price3) in orders_3_pair:
                continue
            amount3 = sum([coins_chain['coin_3']['orderbook'][x].volume for x in range(position + 1)])
            usdAmount3 = amount3 * self.changes[coins_chain['coin_3']['pair'].split('/')[0]]
            depth_chain_3 = {'depth': position,
                             'price': price3,
                             'amount': amount3,
                             'usdAmount': usdAmount3}
            depth_count_3.append(depth_chain_3)
        return depth_count_2, depth_count_3


    def triangles_count(self):
        # print(f"Triangle count started")
        time_start = time.time()
        orderbooks = self._client.raw_order_books
        self.changes_defining()
        triangles = []
        counter = 0
        for triangle in self.triangles_coins:
            # if len(triangles) > 100:
            #     break
            profit_abs_last = {'triangle': None, 'profit_abs': None, 'position': None}
            coins_chain = self.defining_coins_chain(triangle)
            depth_count_2, depth_count_3 = self.defining_depth_counts(coins_chain)
            # if not len(coins_chain['coin_1']['orderbook']) or not len(coins_chain['coin_2']['orderbook']):
            #     continue
            # elif not len(coins_chain['coin_3']['orderbook']):
            #     continue
            for coin_2 in depth_count_2:
                for coin_3 in depth_count_3:
                    counter += 1
                    pair_1 = coins_chain['coin_1']['pair']
                    if triangle['max_order_amount'] > min([coin_2['usdAmount'], coin_3['usdAmount']]):
                        min_amount = min([coin_2['usdAmount'], coin_3['usdAmount']])
                    else:
                        min_amount = triangle['max_order_amount']
                    if min_amount <= 40:
                        continue
                    initial_amount = min_amount / self.changes[coins_chain['coin_1']['coin']]
                    end_amount = (1 + self.profit + self.fee) * initial_amount
                    if coins_chain['coin_2']['side'] == 'SELL':
                        convert_price_2 = coin_2['price']
                    else:
                        convert_price_2 = 1 / coin_2['price']
                    if coins_chain['coin_3']['side'] == 'SELL':
                        convert_price_3 = coin_3['price']
                    else:
                        convert_price_3 = 1 / coin_3['price']
                    # CONVERTATIONS
                    if coins_chain['coin_3']['side'] == 'BUY':
                        amount_3 = end_amount
                    else:
                        amount_3 = end_amount / convert_price_3
                    if coins_chain['coin_3']['side'] == 'BUY':
                        if coins_chain['coin_2']['side'] == 'BUY':
                            amount_2 = amount_3 / convert_price_3
                        elif coins_chain['coin_2']['side'] == 'SELL':
                            amount_2 = amount_3 / convert_price_2 / convert_price_3
                    if coins_chain['coin_3']['side'] == 'SELL':
                        if coins_chain['coin_2']['side'] == 'BUY':
                            amount_2 = amount_3
                        elif coins_chain['coin_2']['side'] == 'SELL':
                            amount_2 = amount_3 / convert_price_2
                    if coins_chain['coin_2']['side'] == 'BUY':
                        if coins_chain['coin_1']['side'] == 'BUY':
                            amount_1 = amount_2 / convert_price_2
                            convert_price_1 = amount_1 / min_amount * self.changes[pair_1.split('/')[1]]
                            main_price = 1 / convert_price_1
                        elif coins_chain['coin_1']['side'] == 'SELL':
                            amount_1 = min_amount / self.changes[pair_1.split('/')[0]]
                            main_price = amount_2 / amount_1 / convert_price_2
                    if coins_chain['coin_2']['side'] == 'SELL':
                        if coins_chain['coin_1']['side'] == 'BUY':
                            amount_1 = amount_2
                            convert_price_1 = amount_1 / min_amount * self.changes[pair_1.split('/')[1]]
                            main_price = 1 / convert_price_1
                        elif coins_chain['coin_1']['side'] == 'SELL':
                            amount_1 = min_amount / self.changes[pair_1.split('/')[0]]
                            main_price = amount_2 / amount_1
                    splited_main_pair = self.splited_pairs[coins_chain['coin_1']['pair']]
                    try:
                        ask_price = orderbooks[splited_main_pair].asks[0].price
                        bid_price = orderbooks[splited_main_pair].bids[0].price
                    except:
                        continue
                    spread = (ask_price - bid_price) / bid_price * 100
                    if coins_chain['coin_1']['side'] == 'SELL':
                        position = ((ask_price - main_price) / (ask_price - bid_price)) * 100
                    else:
                        position = ((main_price - bid_price) / (ask_price - bid_price)) * 100
                    if position < 0:
                        continue
                    profit_abs = (end_amount - initial_amount) * self.changes[coins_chain['coin_1']['coin']]
                    if triangle == profit_abs_last['triangle']:
                        if profit_abs < profit_abs_last['profit_abs']:
                            break
                        elif position < profit_abs_last['position']:
                            break
                    profit_abs_last = {'triangle': triangle, 'profit_abs': profit_abs, 'position': position}
                    # if profit_abs >= 0:
                        # price_len = self.markets[coins_chain['coin_1']['pair']]['precision']['price']
                        # amount_len = self.markets[coins_chain['coin_1']['pair']]['precision']['amount']
                    order_chain = [{'pair': coins_chain['coin_1']['pair'],
                                    'side': coins_chain['coin_1']['side'],
                                    'amount': amount_1 + coins_chain['coin_1']['extra'],
                                    'start_amount': amount_1,
                                    'price': main_price,
                                    'spread': spread,
                                    'position': position,
                                    'main_coin': coins_chain['coin_1']['coin'],
                                    'last_coin': coins_chain['coin_3']['coin'],
                                    'extra': coins_chain['coin_1']['extra'],
                                    'timestamp': None,
                                    'profit_abs': profit_abs},
                                   {'pair': coins_chain['coin_2']['pair'],
                                    'side': coins_chain['coin_2']['side'],
                                    'amount': amount_2,
                                    'price': coin_2['price'],
                                    'depth': coin_2['depth'],
                                    'extra': coins_chain['coin_2']['extra']},
                                   {'pair': coins_chain['coin_3']['pair'],
                                    'side': coins_chain['coin_3']['side'],
                                    'amount': amount_3,
                                    'price': coin_3['price'],
                                    "depth": coin_3['depth'],
                                    'extra': coins_chain['coin_3']['extra']}]
                    triangles.append(order_chain)
                    # print(order_chain[0]['position'])
                    # print()
                        # print(f"{order_chain[0]['pair']} -> {order_chain[1]['pair']} -> {order_chain[2]['pair']}")
                        # print(f"{round(order_chain[0]['position'], 5)} -> {order_chain[1]['depth']} -> {order_chain[2]['depth']}")
                            # print(f"Extras: {coins_chain['coin_1']['extra']} {coins_chain['coin_2']['extra']} {coins_chain['coin_3']['extra']}")
                        # print()
        # print(f"Full time: {time.time() - time_start}")
        # print(f"Cycles: {counter}")
        # print(f'Total triangles found: {len(triangles)}')
        # print()
        return triangles

    def choosing_triangles(self):
        chosen_triangles = {}
        try:
            triangles = self.triangles_count()
        except Exception as e:
            # print(e)
            # print(f"Line 613")
            return
        for triangle in triangles:
            side_pair = triangle[0]['side'] + ' ' + triangle[0]['pair']
            if chosen_triangles.get(side_pair):
                if triangle[0]['position'] > chosen_triangles[side_pair][0]['position']:
                    chosen_triangles[side_pair] = triangle
            else:
                chosen_triangles.update({side_pair: triangle})
        return chosen_triangles


    def change_existing_orders(self):
        chosen_triangles = self.choosing_triangles()
        orders = self._my_orders
        orders_to_cancel = []
        delete_orders = []
        found_orders = []
        new_orders = []
        if chosen_triangles:
            orders_to_compare = set(orders).intersection(chosen_triangles)
            for order_id in orders_to_compare:
                triangle = chosen_triangles[order_id]
                order = orders[order_id]
                best_option = self.define_best_order(triangle, order)
                if best_option == 'triangle':
                    orders_to_cancel.append(order['id'])
                    triangle[0]['timestamp'] = time.time()
                    self.existing_triangles.update({order_id: triangle})
                    new_orders.append(self.create_order_data(triangle[0]))

                    found_orders.append(triangle[0]['side'] + ' ' + triangle[0]['pair'])  # TEST
                    delete_orders.append(order['clientOrderId'])  # TEST

            orders_to_delete = set(orders) - set(chosen_triangles)
            for order_id in orders_to_delete:
                orders_to_cancel.append(orders[order_id]['id'])

                delete_orders.append(orders[order_id]['clientOrderId']) #TEST

            orders_to_create = set(chosen_triangles) - set(orders)
            for order_id in orders_to_create:
                triangle = chosen_triangles[order_id]
                triangle[0]['timestamp'] = time.time()
                self.existing_triangles.update({order_id: triangle})
                new_orders.append(self.create_order_data(triangle[0]))

                found_orders.append(triangle[0]['side'] + ' ' + triangle[0]['pair'])  # TEST
        else:
            for order in orders.values():
                orders_to_cancel.append(order['id'])
            # print(f"New triangles: {set(chosen_triangles)}")
            # print(f"Existed orders: {set(orders)}")
            # print(f"Deleted orders: {delete_orders}")
            # print(f"New orders: {found_orders}")
            # print()
        if len(orders_to_cancel):
            self._client.delete_orders(orders_to_cancel, self.handle_delete_orders)
        if len(new_orders):
            self._client.create_orders(new_orders, self.handle_create_orders)








    # {'START': Order(id='0x48641ca1a25ba5d3089f465faa5d34736552bca5568613d832792bc979c3ada1', symbol='ETHAUDT',
    # side='BUY', type='LIMIT', quantity=36.6, price=1.1, status='CANCELLED', filled_quantity='0',
    # cancelled_quantity='36.6', avg_price=None, client_order_id='START')}

    # Triangle
    # [{'pair': 'TIME/BTC', 'side': 'BUY', 'amount': 1.5930812753410541, 'price': 0.0027896281309955527,
    #   'spread': 16.686935986920904, 'position': 15.439339273691413, 'main_coin': 'BTC', 'last_coin': 'USDT'},
    #  {'pair': 'TIME/USDT', 'side': 'SELL', 'amount': 1.5930812753410541, 'price': 54.71, 'depth': 0},
    #  {'pair': 'BTC/USDT', 'side': 'BUY', 'amount': 0.004484101279719559, 'price': 19437.0, 'depth': 0}]
    #
    # Triangle
    # [{'pair': 'TIME/ETH', 'side': 'SELL', 'amount': 0.6884107570231658, 'price': 0.04556575227324826,
    #   'spread': 8.636990535977558, 'position': 3.316109430971443, 'main_coin': 'TIME', 'last_coin': 'USDT'},
    #  {'pair': 'ETH/USDT', 'side': 'SELL', 'amount': 0.03136795401675687, 'price': 1308.7, 'depth': 0},
    #  {'pair': 'TIME/USDT', 'side': 'BUY', 'amount': 0.6946064538363742, 'price': 59.1, 'depth': 0}]
    #TODO tests
    def define_best_order(self, triangle, order):
        orderbooks = self._client.raw_order_books
        side_pair = triangle[0]['side'] + ' ' + triangle[0]['pair']
        triangle_amount = triangle[0]['amount'] + triangle[0]['extra']
        triangle_amount = self.amount_precision(triangle_amount, triangle[0]['pair'])
        triangle_price = self.price_precision(triangle[0]['price'], triangle[0]['pair'])
        if float(order['quantity']) > triangle_amount:
            return 'triangle'
        if float(order['quantity']) == triangle_amount:
            return 'order'
        if float(order['price']) == triangle_price:
            return 'order'
        if order['side'] == 'BUY':
            if float(order['price']) < orderbooks[order['symbol']].bids[0].price or float(order['price']) < triangle_price:
                return 'triangle'
        else:
            if float(order['price']) > orderbooks[order['symbol']].asks[0].price or float(order['price']) > triangle_price:
                return 'triangle'
        if self.existing_triangles.get(side_pair):
            balancing_triangle = self.existing_triangles[side_pair]
        else:
            return 'triangle'
        return self.find_2_3_liquidity(balancing_triangle)

    #TODO tests
    def find_2_3_liquidity(self, balancing_triangle):
        orderbooks = self._client.raw_order_books
        pair_2 = balancing_triangle[1]['pair']
        price_2 = self.price_precision(balancing_triangle[1]['price'], pair_2)
        side_2 = balancing_triangle[1]['side']
        amount_2 = balancing_triangle[1]['amount']
        pair_2 = self.splited_pairs[pair_2]
        orderbook_2 = orderbooks[pair_2].asks if side_2 == 'BUY' else orderbooks[pair_2].bids
        pair_3 = balancing_triangle[2]['pair']
        price_3 = self.price_precision(balancing_triangle[2]['price'], pair_3)
        side_3 = balancing_triangle[2]['side']
        amount_3 = balancing_triangle[2]['amount']
        pair_3 = self.splited_pairs[pair_3]
        orderbook_3 = orderbooks[pair_3].asks if side_3 == 'BUY' else orderbooks[pair_3].bids
        real_liquid_2 = 0
        found_order = False
        for order_2 in orderbook_2:
            real_liquid_2 += order_2.volume
            if order_2.price == price_2:
                found_order = True
                break
        if not found_order or real_liquid_2 < amount_2:
            return 'triangle'
        real_liquid_3 = 0
        found_order = False
        for order_3 in orderbook_3:
            real_liquid_3 += order_3.volume
            if order_3.price == price_3:
                found_order = True
                break
        if not found_order or real_liquid_3 < amount_3:
            return 'triangle'
        return 'order'

    def price_precision(self, price, pair):
        ticksize = self.markets[pair]['precision']['price']
        if ticksize < 1:
            ticksize_len = len(self.return_string_price(ticksize).split('.')[1])
        else:
            ticksize_len = 0
        price = round(price - (price % ticksize), ticksize_len)
        return price

    def amount_precision(self, amount, pair):
        stepsize = self.markets[pair]['precision']['amount']
        if stepsize < 1:
            stepsize_len = len(self.return_string_price(stepsize).split('.')[1])
        else:
            stepsize_len = 0
        amount = round(amount - (amount % stepsize), stepsize_len)
        return amount

    def create_order_data(self, order, kef = 1):
        ticksize = self.markets[order['pair']]['precision']['price']
        if ticksize < 1:
            ticksize_len = len(self.return_string_price(ticksize).split('.')[1])
        else:
            ticksize_len = 0
        stepsize = self.markets[order['pair']]['precision']['amount']
        if stepsize < 1:
            stepsize_len = len(self.return_string_price(stepsize).split('.')[1])
        else:
            stepsize_len = 0
        amount = order['amount'] * kef
        amount = round(amount - (amount % stepsize), stepsize_len)
        order['price'] = round(order['price'] - (order['price'] % ticksize), ticksize_len)
        if order['side'] == 'BUY':
            side = timex.ORDER_SIDE_BUY
        else:
            side = timex.ORDER_SIDE_SELL
        new_order = timex.NewOrder(
                price=order['price'],
                quantity=amount,
                side=side,
                type=timex.ORDER_TYPE_LIMIT,
                symbol=self.splited_pairs[order['pair']],
                expire_in_seconds=100,
                client_order_id=f'{side} {order["pair"]}')
        return new_order


    def count_start_sum(self, amounts_start):
        changes = self.changes
        coins = [x for x in amounts_start['coins'].split('/') if x != '']
        balances = [x for x in amounts_start['balances'].split('/') if x != '']
        sum_if_no_trades = 0
        for coin, balance in zip(coins, balances):
            sum_if_no_trades += float(balance) * changes[coin]
            amounts_start.update({coin: balance})
        amounts_start.update({'conditTotalUsdBalance': round(sum_if_no_trades)})
        return amounts_start


    def defining_session_start_balance(self):
        database_data = self.dataBase.fetch_data_from_table('balances')
        for record in database_data[::-1]:
            if record[2] == self.session_id:
                amounts_start = {'start_date': record[1],
                                 'coins': record[3],
                                 'balances': record[4],
                                 'usdBalances': record[5],
                                 'usdTotalBalance': record[6]}
                continue
            else:
                amounts_start = self.count_start_sum(amounts_start)
                self.amounts_session_start = amounts_start
                break


    def defining_total_start_balance(self):
        database_data = self.dataBase.fetch_data_from_table('balances')
        first_record = database_data[0]
        amounts_start = {'start_date': first_record[1],
                         'coins': first_record[3],
                         'balances': first_record[4],
                         'usdBalances': first_record[5],
                         'usdTotalBalance': first_record[6]}
        amounts_start = self.count_start_sum(amounts_start)
        self.amounts_total_start = amounts_start


    def check_balance(self):
        balance = self._client.balances
        self.dataBase.sql_balances_update(balance, self.changes, self.session_id)
        if not self.amounts_session_start:
            self.defining_session_start_balance()
        if not self.amounts_total_start:
            self.defining_total_start_balance()
        message = self.balance_message()
        try:
            self.telegram_bot.send_message(self.chat_id, '<pre>' + message + '</pre>', parse_mode='HTML')
        except:
            pass


    def balance_message(self):
        message, now_balance = self.coins_balances_message_creating()
        session_start_balance = self.amounts_session_start['conditTotalUsdBalance']
        project_start_balance = self.amounts_total_start['conditTotalUsdBalance']
        message += f'\nProfits (USD)'

        len_session = 15 - len(f'Session: {round(now_balance - session_start_balance, 2)}')
        message += f'\nSession: {round(now_balance - session_start_balance, 2)}'
        sess_profit_percents = round((now_balance - session_start_balance) / session_start_balance * 100, 2)
        message += ' ' * len_session + f'({sess_profit_percents}%)\n'

        len_proj = 15 - len(f"Prj chg: {round(now_balance - project_start_balance, 2)}")
        message += f"Prj chg: {round(now_balance - project_start_balance, 2)}"
        proj_profit_percents = round((now_balance - project_start_balance) / (project_start_balance) * 100, 2)
        message += ' ' * len_proj + f"({round(proj_profit_percents, 2)}%)"
        return message


    def coins_balances_message_creating(self):
        project_start_balance = self.amounts_total_start['conditTotalUsdBalance']
        balances = self._client.balances
        changes = self.changes
        message_now = f'Current balance\n'
        message_start = f'Start balance\n'
        now_balance = 0
        for balance in balances.values():
            if balance.total_balance == '0':
                continue
            coin = balance.currency
            if coin in ['USDT', 'USDC', 'DAI', 'AUDT', 'USD']:
                precision = 0
            else:
                precision = 4
            string_len = 6 - len(coin)
            now_balance += float(balance.total_balance) * changes[coin]
            amount_start = round(float(self.amounts_session_start[coin]), precision)
            amount_now = round(float(balance.total_balance), precision)
            start_len = 9 - len(str(amount_start))
            total_len = 9 - len(str(amount_now))
            message_start += f"{coin}" + ' ' * string_len + f"{amount_start}"
            message_start += ' ' * start_len + f"({round(amount_start * changes[coin])})\n"
            message_now += f"{coin}" + ' ' * string_len + f"{amount_now}"
            message_now += ' ' * total_len + f"({round(amount_now * changes[coin])})\n"

        message_now += ' ' * 15 + f"({round(now_balance)})"
        message_start += ' ' * 15 + f"({project_start_balance})"
        message = message_start + '\n' + message_now
        return message, now_balance


    def defining_average_balance(self):
        balances = self._client.balances
        changes = self.changes
        total_usd_value = 0
        coins = []
        amounts = []
        for balance in balances.values():
            if balance.total_balance != '0':
                coins.append(balance.currency)
                free_balance = float(balance.total_balance) - float(balance.locked_balance)
                amounts.append({'disbal': float(balance.total_balance) * changes[balance.currency],
                                'free': (free_balance) * changes[balance.currency]
                                })

                total_usd_value += float(balance.total_balance) * changes[balance.currency]
        average_balance = total_usd_value / len(coins)
        for amount in amounts:
            amount['disbal'] = amount['disbal'] - average_balance
        # amounts = map(lambda x: x - average_balance, amounts)
        coins = dict(zip(coins, amounts))
        return average_balance, coins

    def return_string_price(self, price):
        if 'e-' in str(price):
            parts = str(price).split('e-')
            string_price = '0.' + int(float(parts[1]) - 1) * '0' + parts[1]
            return string_price
        else:
            return str(price)

    # def cancel_all_balancing_orders(self):
    #     orders_for_cancel = []
    #     for order in self._my_orders.values():
    #         if 'Balancing' in order['client_order_id:
    #             orders_for_cancel.append(order['id)
    #     if len(orders_for_cancel):
    #         self._client.delete_orders(orders_for_cancel, self.handle_delete_orders)

    #TESTS REQUIRED
    def balancing(self):
        # self.cancel_all_balancing_orders()
        average_balance, coins = self.defining_average_balance()
        orderbooks = self._client.raw_order_books
        pairs = set(self.markets)
        changes = self.changes
        balancing_orders = []
        for first_coin, first_amount in coins.items():
            for second_coin, second_amount in coins.items():
                if first_coin == second_coin:
                    continue
#    if abs(first_amount['disbal']) < 40 and abs(second_amount['total']) < 40:
#       TypeError: 'float' object is not subscriptable
#                 if abs(first_amount['disbal']) < 40 and abs(second_amount['disbal']) < 40:
#                     continue
                balance_pair = None
                if first_amount['disbal'] > 0 and second_amount['disbal'] < 0 or first_amount['disbal'] < 0 and second_amount['disbal'] > 0:
                    for pair in pairs:
                        if first_coin in pair.split('/') and second_coin in pair.split('/'):
                            balance_pair = pair
                            break
                    if not balance_pair:
                        continue
                    if first_amount['disbal'] > 0:
                        sell_coin, buy_coin = first_coin, second_coin
                        sell_amount, buy_amount = first_amount['disbal'], second_amount['disbal']
                        available_amount = first_amount['free']
                    else:
                        sell_coin, buy_coin = second_coin, first_coin
                        sell_amount, buy_amount = second_amount['disbal'], first_amount['disbal']
                        available_amount = second_amount['free']
                    balancing_amount_usd = abs(buy_amount) if abs(buy_amount) < abs(sell_amount) else abs(sell_amount)
                    coins[sell_coin]['disbal'] -= balancing_amount_usd
                    coins[buy_coin]['disbal'] += balancing_amount_usd
                    if sell_coin == balance_pair.split('/')[0]:
                        side, side_4_order = 'SELL', timex.ORDER_SIDE_SELL
                    else:
                        side, side_4_order = 'BUY', timex.ORDER_SIDE_BUY
                    # balance_orderbook = orderbooks[self.splited_pairs[balance_pair]]
                    balancing_amount_usd = balancing_amount_usd if balancing_amount_usd < available_amount else available_amount * 0.95
                    balancing_amount_usd = balancing_amount_usd / changes[balance_pair.split('/')[0]]
                    # ticksize = self.markets[balance_pair]['precision']['price']
                    # if ticksize < 1:
                    #     ticksize_len = len(self.return_string_price(ticksize).split('.')[1])
                    # else:
                    #     ticksize_len = 0
                    stepsize = self.markets[balance_pair]['precision']['amount']
                    if stepsize < 1:
                        stepsize_len = len(self.return_string_price(stepsize).split('.')[1])
                    else:
                        stepsize_len = 0
                    # price = (balance_orderbook.bids[0].price + balance_orderbook.asks[0].price) / 2
                    balancing_amount = round(balancing_amount_usd - (balancing_amount_usd % stepsize), stepsize_len)
                    balancing_orders.append({'pair': balance_pair,
                                             'amount': balancing_amount,
                                             'side': side})
        return balancing_orders


                    #
                    # balancing_orders.append(
                    #     timex.NewOrder(
                    #         price=price,
                    #         quantity=balancing_amount,
                    #         side=side_4_order,
                    #         type=timex.ORDER_TYPE_LIMIT,
                    #         symbol=self.splited_pairs[balance_pair],
                    #         expire_in_seconds=100,
                    #         client_order_id=f'Balancing {sell_coin}->{buy_coin}'
                    #     ))
                    # print(timex.NewOrder(
                        #     price=price,
                        #     quantity=balancing_amount,
                        #     side=side_4_order,
                        #     type=timex.ORDER_TYPE_LIMIT,
                        #     symbol=self.splited_pairs[balance_pair],
                        #     expire_in_seconds=100,
                        #     client_order_id=f'Balancing {sell_coin}->{buy_coin}',
                        # ))
        # self._client.create_orders(balancing_orders, self.handle_create_orders)



timex_client = timex.WsClientTimex(cp["TIMEX"]["api_key"], cp["TIMEX"]["api_secret"])
bot = TriangleBot(timex_client)
bot.run()

