import collections
import logging
import configparser
import sys
import uuid
import ccxt
import time
import random

import py_timex.client as timex

cp = configparser.ConfigParser()

if len(sys.argv) != 2:
    print("Usage %s <config.ini>" % sys.argv[0])
    sys.exit(1)
cp.read(sys.argv[1], "utf-8")

FORMAT = '%(asctime)s %(levelname)s %(filename)s:%(lineno)d %(message)s (%(funcName)s)'
logging.basicConfig(format=FORMAT)
log = logging.getLogger("sample bot")
log.setLevel(logging.DEBUG)

class TriangleBot:
    _raw_updates = 0
    _group_updates = 0

    profit = 0.001
    bot = ccxt.timex({})
    markets = bot.load_markets()

    pairs = set(markets)
    pairs.discard('TIMEV1/BTC')
    pairs.discard('TIMEV1/ETH')
    splited_pairs = {}

    changes = {'USD': 1, 'USDT': 1, 'USDC': 1}
    start_time = time.time()
    triangles_coins = []
    coins = []

    def __init__(self, client: timex.WsClientTimex):
        self._my_orders = dict[str: timex.Order]
        self._client = client
        self.depth = 2
        client.on_first_connect = self.on_first_connect
        client.subscribe_balances(self.handle_balance)
        client.subscribe_orders(self.handle_order)
        # client.subscribe_group_order_book(timex.ETHAUDT, self.handle_group_order_book_update)
        for pair in self.pairs:
            self.splited_pairs.update({pair: pair.split('/')[0] + pair.split('/')[1]})
            client.subscribe_raw_order_book(self.splited_pairs[pair], self.handle_raw_order_book_update)

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
        if not self._raw_updates % 100:
            try:
                self.find_all_triangles()
            except:
                return
        if not self._raw_updates % 5:
            balance = self._client.balances
            # print(balance)
            self.triangles_count()
            print(f"Triangle count ended")
            # print(f"Total updates: {self._raw_updates}\nTime since started: {time.time() - self.start_time}")


    def on_first_connect(self):
        self.find_all_triangles()
        self._client.create_orders([
            timex.NewOrder(
                price=1.1,
                quantity=36.6,
                side=timex.ORDER_SIDE_BUY,
                type=timex.ORDER_TYPE_LIMIT,
                symbol=timex.ETHAUDT,
                expire_in_seconds=3,
                client_order_id=str(uuid.uuid4()),
            )], self.handle_create_orders)

    def handle_create_orders(self, obj):
        # log.info(obj)
        pass

    def handle_balance(self, balance: timex.Balance):
        # log.info(balance)
        pass

    def handle_order(self, order: timex.Order):
        self._my_orders.update({order.client_order_id: order})
        log.info(order)
        self._client.delete_orders([order.id], self.handle_delete_orders)

    def handle_delete_orders(self, obj):
        log.info(obj)

    def run(self):
        try:
            return self._client.run_updater()
        except KeyboardInterrupt:
            self._client.wait_closed()

    def find_all_triangles(self):
        pairs = self.pairs
        orderbooks = self._client.raw_order_books
        triangle_sets = []
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
                        self.triangles_coins.append({'coins': set(all_coins), 'pairs': [pair_1, pair_2, pair_3]})

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
        pairs = self.pairs
        coins = []
        for pair in pairs:
            coins.append(pair.split('/')[0])
            coins.append(pair.split('/')[1])
        self.coins = set(coins)

    def sorting_triangles(self):
        new_triangles_coins = []
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
                                new_triangles_coins.update({'coins': [coin_1, coin_2, coin_3],
                                                            'pairs': [pair_1, pair_2, pair_3]})

        self.triangles_coins = new_triangles_coins

    def finding_audt_change_price(self):
        orderbooks = self._client.raw_order_books
        btcusd_price = (orderbooks['BTCUSD'].bids[0].price + orderbooks['BTCUSD'].asks[0].price) / 2
        btcaudt_price = (orderbooks['BTCAUDT'].bids[0].price + orderbooks['BTCAUDT'].asks[0].price) / 2
        ethusd_price = (orderbooks['ETHUSD'].bids[0].price + orderbooks['ETHUSD'].asks[0].price) / 2
        ethaudt_price = (orderbooks['ETHAUDT'].bids[0].price + orderbooks['ETHAUDT'].asks[0].price) / 2
        change_AUDT = round((ethusd_price / ethaudt_price + btcusd_price / btcaudt_price) / 2, 3)
        self.changes.update({'AUDT': change_AUDT})

    def changes_define(self):
        orderbooks = self._client.raw_order_books
        self.finding_audt_change_price()
        for coin in self.coins:
            if 'USD' in coin:
                continue
            if coin == 'AUDT':
                pass
            # orderbooks. update({coin + '/USD': my_fetch_order_book_waves(coin + '/USDN', 20)})
            for x in ['USDT', 'USD', 'USDC']:
                try:
                    change = (orderbooks[coin + x].bids[0].price + orderbooks[coin + x].asks[0].price) / 2
                    self.changes.update({coin: change})
                    break
                except:
                    pass
        # print(changes)
            # all_amounts.update({coin: balance['total'][coin] * changes[coin]})

    def change_max_order_amount():
        balance = fetch_balance()
        all_coins = set(balance['total'])
        convert_orderbooks = {'USDN': 1}
        orderbooks = {}
        all_amounts = {}
        MAX_ORDER_AMOUNTS = []
        for coin in all_coins:
            if coin == 'USDN':
                all_amounts.update({coin: balance['total']['USDN']})
                continue
            orderbooks.update({coin + '/USDN': my_fetch_order_book_waves(coin + '/USDN', 20)})
            convert_orderbooks.update({coin: orderbooks[coin + '/USDN']['bids'][1][0]})
            all_amounts.update({coin: balance['total'][coin] * convert_orderbooks[coin]})
        for coin_1 in all_coins:
            for coin_2 in all_coins:
                amounts = []
                if coin_1 == coin_2:
                    continue
                amounts.append(all_amounts[coin_1])
                amounts.append(all_amounts[coin_2])
                MAX_ORDER_AMOUNTS.append({'coins': [coin_1, coin_2], 'max_amount': round(min(amounts) * 0.99)})
        if blocked_coins:
            for block_coin in blocked_coins:
                for max_amount in MAX_ORDER_AMOUNTS:
                    if block_coin['coin'] in max_amount['coins']:
                        max_amount['max_amount'] -= block_coin['amount']
        return MAX_ORDER_AMOUNTS, convert_orderbooks, orderbooks


    def defining_coins_chain(self, triangle):
        orderbooks = self._client.raw_order_books
        coin_1, coin_2, coin_3 = triangle['coins'][0], triangle['coins'][1], triangle['coins'][2]
        pair_1, pair_2, pair_3 = triangle['pairs'][0], triangle['pairs'][1], triangle['pairs'][2]
        coins_chain = {}
        # Формирование первого главного коина в словаре
        if coin_1 == pair_1.split('/')[0]:
            coins_chain.update({'coin_1': {'coin': coin_1,
                                           'pair': pair_1,
                                           'side': 'sell',
                                           'orderbook': orderbooks[self.splited_pairs[pair_1]].asks,
                                           'price': self.changes[coin_1]}})
        else:
            coins_chain.update({'coin_1': {'coin': coin_1,
                                           'pair': pair_1,
                                           'side': 'buy',
                                           'orderbook': orderbooks[self.splited_pairs[pair_1]].bids,
                                           'price': self.changes[coin_1]}})
        # Формирование 2 и 3 коина в словаре
        for num, coin, pair in [(2, coin_2, pair_2), (3, coin_3, pair_3)]:
            if coin == pair.split('/')[0]:
                side = 'sell'
                orderbook = orderbooks[self.splited_pairs[pair]].bids
            else:
                side = 'buy'
                orderbook = orderbooks[self.splited_pairs[pair]].asks
            coins_chain.update({'coin_' + str(num): {'coin': coin,
                                                     'pair': pair,
                                                     'side': side,
                                                     'orderbook': orderbook}})
        return coins_chain

    def defining_depth_counts(self, coins_chain):
        # просчет размеров лота в глубину
        depth_count_2 = []
        depth_count_3 = []
        for position in range(self.depth):
            try:
                amount_2 = sum([coins_chain['coin_2']['orderbook'][x].volume for x in range(position + 1)])
            except:
                print(coins_chain['coin_2']['orderbook'])
            usdAmount_2 = amount_2 * self.changes[coins_chain['coin_2']['pair'].split('/')[0]]
            price_2 = coins_chain['coin_2']['orderbook'][position].price
            depth_chain_2 = {'depth': position,
                             'price': price_2,
                             'amount': amount_2,
                             'usdAmount': usdAmount_2}
            depth_count_2.append(depth_chain_2)
            amount_3 = sum([coins_chain['coin_3']['orderbook'][x].volume for x in range(position + 1)])
            usdAmount_3 = amount_3 * self.changes[coins_chain['coin_3']['pair'].split('/')[0]]
            price_3 = coins_chain['coin_3']['orderbook'][position].price
            depth_chain_3 = {'depth': position,
                             'price': price_3,
                             'amount': amount_3,
                             'usdAmount': usdAmount_3}
            depth_count_3.append(depth_chain_3)
        return depth_count_2, depth_count_3

    def triangles_count(self):
        print(f"Triangle count started")
        time_start = time.time()
        orderbooks = self._client.raw_order_books
        self.changes_define()
        order_amounts = None
        triangles = []
        for triangle in self.triangles_coins:
            coins_chain = self.defining_coins_chain(triangle)
            depth_count_2, depth_count_3 = self.defining_depth_counts(coins_chain)
            for coin_2 in depth_count_2:
                for coin_3 in depth_count_3:
                    min_amount = min([coin_2['usdAmount'], coin_3['usdAmount']])
                    if min_amount <= 0:
                        continue
                    fee = min_amount * 0.005
                    initial_amount = (min_amount + fee) / self.changes[coins_chain['coin_1']['coin']]
                    end_amount = (1 + self.profit) * initial_amount
                    if coins_chain['coin_2']['side'] == 'sell':
                        convert_price_2 = coin_2['price']
                    else:
                        convert_price_2 = 1 / coin_2['price']
                    if coins_chain['coin_3']['side'] == 'sell':
                        convert_price_3 = coin_3['price']
                    else:
                        convert_price_3 = 1 / coin_3['price']
                    # CONVERTATIONS
                    if coins_chain['coin_3']['side'] == 'buy':
                        amount_3 = end_amount
                    else:
                        amount_3 = end_amount / convert_price_3
                    if coins_chain['coin_3']['side'] == 'buy':
                        if coins_chain['coin_2']['side'] == 'buy':
                            amount_2 = amount_3 / convert_price_3
                        elif coins_chain['coin_2']['side'] == 'sell':
                            amount_2 = amount_3 / convert_price_2 / convert_price_3
                    if coins_chain['coin_3']['side'] == 'sell':
                        if coins_chain['coin_2']['side'] == 'buy':
                            amount_2 = amount_3
                        elif coins_chain['coin_2']['side'] == 'sell':
                            amount_2 = amount_3 / convert_price_2
                    if coins_chain['coin_2']['side'] == 'buy':
                        if coins_chain['coin_1']['side'] == 'buy':
                            amount_1 = amount_2 / convert_price_2
                            convert_price_1 = amount_1 / min_amount * self.changes[pair_1.split('/')[1]]
                            main_price = 1 / convert_price_1
                        elif coins_chain['coin_1']['side'] == 'sell':
                            amount_1 = min_amount / self.changes[pair_1.split('/')[0]]
                            # print(f"amount_2:{amount_2} amount_1:{amount_1} convert_price_2:{convert_price_2}")
                            main_price = amount_2 / amount_1 / convert_price_2
                    if coins_chain['coin_2']['side'] == 'sell':
                        if coins_chain['coin_1']['side'] == 'buy':
                            amount_1 = amount_2
                            convert_price_1 = amount_1 / min_amount * self.changes[pair_1.split('/')[1]]
                            main_price = 1 / convert_price_1
                        elif coins_chain['coin_1']['side'] == 'sell':
                            amount_1 = min_amount / self.changes[pair_1.split('/')[0]]
                            main_price = amount_2 / amount_1
                    if coins_chain['coin_1']['side'] == 'sell':
                        splited_main_pair = self.splited_pairs[coins_chain['coin_1']['pair']]
                        ask_price = orderbooks[splited_main_pair].asks[0].price
                        bid_price = orderbooks[splited_main_pair].bids[0].price
                        spread = (ask_price - bid_price) / bid_price * 100
                        position = (ask_price - main_price) / (ask_price - bid_price) * 100
                        # print(f"Main price: {main_price}\nTop ask price: {ask_price}")
                        if main_price > coins_chain['coin_1']['orderbook'][0].price:
                            continue
                    else:
                        splited_main_pair = self.splited_pairs[coins_chain['coin_1']['pair']]
                        ask_price = orderbooks[splited_main_pair].asks[0].price
                        bid_price = orderbooks[splited_main_pair].bids[0].price
                        spread = (ask_price - bid_price) / ask_price * 100
                        position = (main_price - bid_price) / (ask_price - bid_price) * 100
                        # print(f"Main price: {main_price}\nTop bid price: {bid_price}")
                        if main_price < coins_chain['coin_1']['orderbook'][0].price:
                            continue
                    profit_abs = (end_amount - initial_amount) * self.changes[coins_chain['coin_1']['coin']]
                    # print(f"Profit abs: {profit_abs}\nTriangle: {[pair_1, pair_2, pair_3]}")
                    # print(f"Position in spread: {position}%")
                    # print()
                    # if coin_3 == profit_abs_last['coin']:
                    #     if profit_abs < profit_abs_last['profit_abs']:
                    #         if coin_3 != depth_count_3[-1]:
                    #             break
                    # profit_abs_last = {'coin': coin_3, 'profit_abs': profit_abs}
                    if profit_abs >= 0:
                        # price_len = self.markets[coins_chain['coin_1']['pair']]['precision']['price']
                        # amount_len = self.markets[coins_chain['coin_1']['pair']]['precision']['amount']
                        order_chain = [{'pair': coins_chain['coin_1']['pair'],
                                        'side': coins_chain['coin_1']['side'],
                                        'amount': amount_1,
                                        'price': main_price,
                                        'spread': spread,
                                        'position': position,
                                        'main_coin': coin_1,
                                        'last_coin': coin_3,
                                        'changes': self.changes},
                                       {'pair': coins_chain['coin_2']['pair'],
                                        'side': coins_chain['coin_2']['side'],
                                        'amount': amount_2,
                                        'price': coin_2['price'],
                                        'depth': coin_2['depth']},
                                       {'pair': coins_chain['coin_3']['pair'],
                                        'side': coins_chain['coin_3']['side'],
                                        'amount': amount_3,
                                        'price': coin_3['price'],
                                        "depth": coin_3['depth']}]
                        triangles.append([order_chain, profit_abs])
        # print(f"All triangles: {working_triangles}")
        # print(f"Number of triangles: {len(working_triangles)}")
        # coins = []
        # for one in working_triangles:
        #     for pair in one:
        #         coins.append(pair.split('/')[0])
        #         coins.append(pair.split('/')[1])
        # coins = set(coins)
        # print(f"All working coins: {coins}")
        # print(f"Number of working coins: {len(coins)}")
        print(f"Full time: {time.time() - time_start}")
        print(f'Total triangles found: {len(triangles)}')

timex_client = timex.WsClientTimex(cp["TIMEX"]["api_key"], cp["TIMEX"]["api_secret"])
bot = TriangleBot(timex_client)
bot.run()

