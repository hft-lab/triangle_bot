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

    bot = ccxt.timex({})
    markets = bot.load_markets()
    pairs = set(markets)
    pairs.discard('TIMEV1/BTC')
    pairs.discard('TIMEV1/ETH')
    splited_pairs = {}
    changes = {'USD': 1, 'USDT': 1, 'USDC': 1,}

    def __init__(self, client: timex.WsClientTimex):
        self._my_orders = dict[str: timex.Order]
        self._client = client
        self.depth = 2
        client.on_first_connect = self.on_first_connect
        # client.subscribe(timex.ETHAUDT, self.handle_update)
        # client.subscribe(timex.BTCUSD, self.handle_update)
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
        if not self._raw_updates % 5:
            self.triangles_count()
            print(f"Triangle count ended")
        # print(orderbooks['ETHAUDT']['asks'])
        # print(orderbooks['ETHAUDT']['bids'])

    def on_first_connect(self):
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
        # coins = []
        # for pair in data:
        #     # pairs.append(data[pair]['id'])
        #     coins.append(data[pair]['baseId'])
        #     coins.append(data[pair]['quoteId'])
        # coins = set(coins)
        # coins.discard('TIMEV1')
        triangles_coins = []
        triangle_sets = []
        for pair_1 in pairs:
            for pair_2 in pairs:
                for pair_3 in pairs:
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
                    # map(['COMP'], lambda x: if x in all_coins continue)
                    flag = False
                    for coin in all_coins:
                        if all_coins.count(coin) != 2:
                            flag = True
                    if flag:
                        flag = False
                        continue
                    if {pair_1, pair_2, pair_3} not in triangle_sets:
                        # flag = False
                        # for pair in {pair_1, pair_2, pair_3}:
                        #     try:
                        #         orderbooks[pair].bids
                        #         orderbooks[pair].asks
                        #     except Exception as e:
                        #         # print(f"142: {e}")
                        #         flag = True
                        #         break
                        # if flag:
                        #     continue
                        triangle_sets.append({pair_1, pair_2, pair_3})
                        triangles_coins.append({'coins': set(all_coins),
                                                'pairs': [pair_1, pair_2, pair_3]})

        # print(triangle_sets)
        pairs = []
        for triangle in triangle_sets:
            for pair in triangle:
                pairs.append(pair)
        pairs = set(pairs)
        # print(f"Total pairs: {len(pairs)}")

        coins = []
        for pair in pairs:
            coins.append(pair.split('/')[0])
            coins.append(pair.split('/')[1])
        coins = set(coins)
        return triangles_coins, pairs, coins

    def finding_audt_change_price(self):
        orderbooks = self._client.raw_order_books
        btcusd_price = (orderbooks['BTCUSD'].bids[0].price + orderbooks['BTCUSD'].asks[0].price) / 2
        btcaudt_price = (orderbooks['BTCAUDT'].bids[0].price + orderbooks['BTCAUDT'].asks[0].price) / 2
        ethusd_price = (orderbooks['ETHUSD'].bids[0].price + orderbooks['ETHUSD'].asks[0].price) / 2
        ethaudt_price = (orderbooks['ETHAUDT'].bids[0].price + orderbooks['ETHAUDT'].asks[0].price) / 2
        change_AUDT = round((ethusd_price / ethaudt_price + btcusd_price / btcaudt_price) / 2, 3)
        self.changes.update({'AUDT': change_AUDT})

    def changes_define(self, coins, pairs):
        orderbooks = self._client.raw_order_books
        self.finding_audt_change_price()
        for coin in coins:
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

    def triangles_count(self):
        print(f"Triangle count started")
        orderbooks = self._client.raw_order_books
        try:
            triangles_coins, pairs, coins = self.find_all_triangles()
            self.changes_define(coins, pairs)
        except:
            return
        triangles = []
        time_start = time.time()
        for triangle in triangles_coins:
            for coin_1 in triangle['coins']:
                for pair_1 in triangle['pairs']:
                    if not len(orderbooks[self.splited_pairs[pair_1]].bids):
                        continue
                    if not len(orderbooks[self.splited_pairs[pair_1]].asks):
                        continue
                    for pair_2 in triangle['pairs']:
                        if not len(orderbooks[self.splited_pairs[pair_2]].bids):
                            continue
                        if not len(orderbooks[self.splited_pairs[pair_2]].asks):
                            continue
                        for pair_3 in triangle['pairs']:
                            if not len(orderbooks[self.splited_pairs[pair_3]].bids):
                                continue
                            if not len(orderbooks[self.splited_pairs[pair_3]].asks):
                                continue
                            if pair_1 == pair_2 or pair_1 == pair_3 or pair_2 == pair_3:
                                continue
                            # if triangle['pairs'] not in working_triangles:
                            #     working_triangles.append(triangle['pairs'])
                            if coin_1 in pair_1 and coin_1 in pair_3:
                                coins_chain = {}
                                # Формирование первого главного коина в словаре
                                if coin_1 == pair_1.split('/')[0]:
                                    coins_chain.update({'coin_1': {'coin': coin_1,
                                                                   'pair': pair_1,
                                                                   'side': 'sell',
                                                                   'orderbook_side': 'asks',
                                                                   'orderbook': orderbooks[self.splited_pairs[pair_1]].asks,
                                                                   'price': orderbooks[self.splited_pairs[pair_1]].asks[0].price}})
                                    coin_2 = pair_1.split('/')[1]
                                else:
                                    coins_chain.update({'coin_1': {'coin': coin_1,
                                                                   'pair': pair_1,
                                                                   'side': 'buy',
                                                                   'orderbook_side': 'bids',
                                                                   'orderbook': orderbooks[self.splited_pairs[pair_1]].bids,
                                                                   'price': orderbooks[self.splited_pairs[pair_1]].bids[0].price}})
                                    coin_2 = pair_1.split('/')[0]
                                coin_3 = [x for x in triangle['coins'] if x not in [coin_1, coin_2]][0]
                                # MAX_ORDER_AMOUNT = [x['max_amount'] for x in MAX_ORDER_AMOUNTS if coin_1 in x['coins']
                                # and coin_3 in x['coins']][0]
                                # Формирование 2 и 3 коина в словаре
                                for num, coin, pair in [(2, coin_2, pair_2), (3, coin_3, pair_3)]:
                                    if coin == pair.split('/')[0]:
                                        side, orderbook_side = 'sell', 'bids'
                                        orderbook = orderbooks[self.splited_pairs[pair]].bids
                                    else:
                                        side, orderbook_side = 'buy', 'asks'
                                        orderbook = orderbooks[self.splited_pairs[pair]].asks
                                    coins_chain.update({'coin_' + str(num): {'coin': coin,
                                                                             'pair': pair,
                                                                             'side': side,
                                                                             'orderbook_side': orderbook_side,
                                                                             'orderbook': orderbook}})
                                # просчет размеров лота в глубину
                                depth_count_2 = []
                                depth_count_3 = []

                                for position in range(self.depth):
                                    # try:
                                    position = 0
                                    amount = sum([coins_chain['coin_2']['orderbook'][x].volume for x in range(position + 1)])
                                    # [coins_chain['coin_' + str(2)]['orderbook'][x].volume for x in range(position) if
                                    #  coins_chain['coin_' + str(2)]['orderbook'][x].volume not in last_orders_amounts])
                                    usdAmount = amount * self.changes[coins_chain['coin_2']['pair'].split('/')[0]]
                                    price = coins_chain['coin_2']['orderbook'][position].price
                                    # except:
                                    #     break
                                    # if price in last_orders_prices or amount == 0:
                                    # continue
                                    depth_chain = {'depth': position,
                                                   'price': price,
                                                   'amount': amount,
                                                   'usdAmount': usdAmount}
                                    # depth_chain['usdAmount'] = MAX_ORDER_AMOUNT if depth_chain['usdAmount'] >
                                    # MAX_ORDER_AMOUNT else depth_chain['usdAmount']
                                    # depth_chain['usdAmount'] = depth_chain['usdAmount']
                                    depth_count_2.append(depth_chain)
                                    # if depth_chain['usdAmount'] == MAX_ORDER_AMOUNT:
                                    #     if position != depth - 1:
                                    #         break

                                for position in range(self.depth):
                                    # try:
                                    position = 0
                                    amount = sum([coins_chain['coin_3']['orderbook'][x].volume for x in range(position + 1)])
                                    # coins_chain['coin_' + str(3)]['orderbook'][x].volume not in last_orders_amounts])
                                    usdAmount = amount * self.changes[coins_chain['coin_3']['pair'].split('/')[0]]
                                    # usdAmount = sum([coins_chain['coin_' + str(3)]['orderbook'][x].volume for x in
                                    # range(position) if coins_chain['coin_' + str(3)]['orderbook'][x].volume not in last_
                                    # orders_amounts]) * changes[coins_chain['coin_' + str(3)]['pair'].split('/')[0]]
                                    price = coins_chain['coin_3']['orderbook'][position].price
                                    # except:
                                    #     break
                                    # if price in last_orders_prices or amount == 0:
                                    #     continue
                                    depth_chain = {'depth': position,
                                                   'price': price,
                                                   'amount': amount,
                                                   'usdAmount': usdAmount}
                                    # depth_chain['usdAmount'] = MAX_ORDER_AMOUNT if depth_chain['usdAmount'] >
                                    # MAX_ORDER_AMOUNT else depth_chain['usdAmount']
                                    # depth_chain['usdAmount'] = depth_chain['usdAmount']
                                    depth_count_3.append(depth_chain)
                                    # if depth_chain['usdAmount'] == MAX_ORDER_AMOUNT:
                                    #     if position != depth - 1:
                                    #         break

                                for coin_2 in depth_count_2:
                                    for coin_3 in depth_count_3:
                                        min_amount = min([coin_2['usdAmount'], coin_3['usdAmount']])
                                        # if coin_2['usdAmount'] < coin_3['usdAmount']:
                                        #     min_amount = coin_2['usdAmount']
                                        # else:
                                        #     min_amount = coin_3['usdAmount']
                                        if min_amount <= 0:
                                            print(f"MIN AMOUNT LESS THAN 0: {min_amount}")
                                            continue
                                        fee = min_amount * 0.004
                                        initial_amount = (min_amount + fee) / self.changes[coins_chain['coin_1']['coin']]

                                        # profit = random.choice([x * 0.0005 for x in range(2, 11)])
                                        profit = 0.001
                                        end_amount = (1 + profit) * initial_amount
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

