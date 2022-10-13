import collections
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
    changes = {'USD': 1, 'USDT': 1, 'USDC': 1}
    amounts_session_start = None
    amounts_total_start = None

    start_time = time.time()

    def __init__(self, client: timex.WsClientTimex):
        self.session_id = self.id_generator(size=6)
        self._my_orders = dict[str: timex.Order]
        self._client = client
        self.depth = 5
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
            balances = self._client.balances
            changes = self.changes
            self.dataBase.sql_balances_update(balances, changes, self.session_id)
            self.check_balance()
            self.balancing()

        if self._raw_updates > 199:
            if not self._raw_updates % 100:
                self.find_all_triangles()
            if not self._raw_updates % 5:
                self.triangles_count()
                print(f"Triangle count ended")
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
                client_order_id=str(uuid.uuid4()),
            )], self.handle_create_orders)


    def handle_create_orders(self, obj):
        log.info(obj)
        pass


    def handle_balance(self, balance: timex.Balance):
        self.sorting_triangles()
        # log.info(balance)
        pass


    def handle_order(self, order: timex.Order):
        self._my_orders.update({order.client_order_id: order})
        log.info(order)
        # self._client.delete_orders([order.id], self.handle_delete_orders)


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
        if btcusd_price and btcaudt_price:
            change_AUDT = round((btcusd_price / btcaudt_price) / 2, 3)
        if ethusd_price and ethaudt_price:
            change_AUDT = round((ethusd_price / ethaudt_price) / 2, 3)
        self.changes.update({'AUDT': change_AUDT})


    def changes_defining(self):
        orderbooks = self._client.raw_order_books
        self.finding_audt_change_price()
        for coin in self.coins:
            if 'USD' in coin:
                continue
            if coin == 'AUDT':
                pass
            for x in ['USDT', 'USD', 'USDC']:
                try:
                    change = self.defining_middle_spread_price(orderbooks, coin + x)
                    self.changes.update({coin: change})
                    break
                except:
                    pass


    def defining_coins_chain(self, triangle):
        orderbooks = self._client.raw_order_books
        coin_1, coin_2, coin_3 = triangle['coins'][0], triangle['coins'][1], triangle['coins'][2]
        pair_1, pair_2, pair_3 = triangle['pairs'][0], triangle['pairs'][1], triangle['pairs'][2]
        coins_chain = {}
        # defining the first main coin in dict
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
        # defining second and third coins in dict
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
        # counting into deep of liquidity
        depth_count_2 = []
        depth_count_3 = []
        for position in range(self.depth):
            amount2 = sum([coins_chain['coin_2']['orderbook'][x].volume for x in range(position + 1)])
            usdAmount2 = amount2 * self.changes[coins_chain['coin_2']['pair'].split('/')[0]]
            price2 = coins_chain['coin_2']['orderbook'][position].price
            depth_chain_2 = {'depth': position,
                             'price': price2,
                             'amount': amount2,
                             'usdAmount': usdAmount2}
            amount3 = sum([coins_chain['coin_3']['orderbook'][x].volume for x in range(position + 1)])
            usdAmount3 = amount3 * self.changes[coins_chain['coin_3']['pair'].split('/')[0]]
            price3 = coins_chain['coin_3']['orderbook'][position].price
            depth_chain_3 = {'depth': position,
                             'price': price3,
                             'amount': amount3,
                             'usdAmount': usdAmount3}
            depth_count_2.append(depth_chain_2)
            depth_count_3.append(depth_chain_3)
        return depth_count_2, depth_count_3


    def triangles_count(self):
        print(f"Triangle count started")
        time_start = time.time()
        orderbooks = self._client.raw_order_books
        self.changes_defining()
        triangles = []
        for triangle in self.triangles_coins:
            profit_abs_last = {'coin': None, 'profit_abs': None}
            coins_chain = self.defining_coins_chain(triangle)
            depth_count_2, depth_count_3 = self.defining_depth_counts(coins_chain)
            for coin_2 in depth_count_2:
                for coin_3 in depth_count_3:
                    pair_1 = coins_chain['coin_1']['pair']
                    if triangle['max_order_amount'] > min([coin_2['usdAmount'], coin_3['usdAmount']]):
                        min_amount = min([coin_2['usdAmount'], coin_3['usdAmount']])
                    else:
                        min_amount = triangle['max_order_amount']
                    if min_amount <= 0:
                        continue
                    fee = min_amount * 0.004
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
                        if main_price > coins_chain['coin_1']['orderbook'][0].price:
                            continue
                    else:
                        splited_main_pair = self.splited_pairs[coins_chain['coin_1']['pair']]
                        ask_price = orderbooks[splited_main_pair].asks[0].price
                        bid_price = orderbooks[splited_main_pair].bids[0].price
                        spread = (ask_price - bid_price) / ask_price * 100
                        position = (main_price - bid_price) / (ask_price - bid_price) * 100
                        if main_price < coins_chain['coin_1']['orderbook'][0].price:
                            continue
                    profit_abs = (end_amount - initial_amount) * self.changes[coins_chain['coin_1']['coin']]
                    if coin_3 == profit_abs_last['coin']:
                        if profit_abs < profit_abs_last['profit_abs']:
                            break
                    profit_abs_last = {'coin': coin_3, 'profit_abs': profit_abs}
                    if profit_abs >= 0:
                        # price_len = self.markets[coins_chain['coin_1']['pair']]['precision']['price']
                        # amount_len = self.markets[coins_chain['coin_1']['pair']]['precision']['amount']
                        order_chain = [{'pair': coins_chain['coin_1']['pair'],
                                        'side': coins_chain['coin_1']['side'],
                                        'amount': amount_1,
                                        'price': main_price,
                                        'spread': spread,
                                        'position': position,
                                        'main_coin': coins_chain['coin_1']['coin'],
                                        'last_coin': coins_chain['coin_3']['coin'],
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
        print(f"Full time: {time.time() - time_start}")
        print(f'Total triangles found: {len(triangles)}')


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
                amounts.append(float(balance.total_balance) * changes[balance.currency])
                total_usd_value += float(balance.total_balance) * changes[balance.currency]
        average_balance = total_usd_value / len(coins)
        amounts = map(lambda x: x - average_balance, amounts)
        coins = dict(zip(coins, amounts))
        return average_balance, coins


    def balancing(self):
        average_balance, coins = self.defining_average_balance()
        orderbooks = self._client.raw_order_books
        pairs = set(self.markets)
        changes = self.changes
        balancing_orders = []
        for first_coin, first_amount in coins.items():
            for second_coin, second_amount in coins.items():
                if first_coin == second_coin:
                    continue
                if average_balance * 0.03 > abs(first_amount) or average_balance * 0.03 > abs(second_amount):
                    continue
                balance_pair = None
                if first_amount > 0 and second_amount < 0 or first_amount < 0 and second_amount > 0:
                    for pair in pairs:
                        if first_coin in pair.split('/') and second_coin in pair.split('/'):
                            balance_pair = pair
                            break
                    if not balance_pair:
                        continue
                    if first_amount > 0:
                        sell_coin, buy_coin, sell_amount, buy_amount = first_coin, second_coin, first_amount, second_amount
                    else:
                        sell_coin, buy_coin, sell_amount, buy_amount = second_coin, first_coin, second_amount, first_amount
                    balancing_amount_usd = abs(buy_amount) if abs(buy_amount) < abs(sell_amount) else abs(sell_amount)
                    coins[sell_coin] -= balancing_amount_usd
                    coins[buy_coin] += balancing_amount_usd
                    if sell_coin == balance_pair.split('/')[0]:
                        side, side_4_order = 'sell', timex.ORDER_SIDE_SELL
                    else:
                        side, side_4_order = 'buy', timex.ORDER_SIDE_BUY
                    balance_orderbook = orderbooks[self.splited_pairs[balance_pair]]
                    balancing_amount = balancing_amount_usd / changes[balance_pair.split('/')[0]]
                    ticksize = self.markets[balance_pair]['precision']['price']
                    price = (balance_orderbook.bids[0].price + balance_orderbook.asks[0].price) / 2
                    price = price - (price % ticksize)
                    print(f"Balance pair: {balance_pair}\nSide: {side}\nPrice: {price}")
                    print(f"Sell coin: {sell_coin}\nBuy coin: {buy_coin}\nAmount, USD: {balancing_amount_usd}")
                    balancing_orders.append(
                        timex.NewOrder(
                            price=price,
                            quantity=balancing_amount,
                            side=side_4_order,
                            type=timex.ORDER_TYPE_LIMIT,
                            symbol=self.splited_pairs[balance_pair],
                            expire_in_seconds=100,
                            client_order_id=f'Balancing {sell_coin}->{buy_coin}',
                        ))
        self._client.create_orders(balancing_orders, self.handle_create_orders)
                    # place_order({'pair': balance_pair, 'side': side, 'amount': balancing_amount, 'price': price})
                    # if side == 'sell':
                    #     blocked_coins.append({'coin': balance_pair.split('/')[0], 'amount': balancing_amount * orderbooks[balance_pair.split('/')[0]], 'pair': balance_pair, 'side': side})
                    # else:
                    #     blocked_coins.append({'coin': balance_pair.split('/')[1], 'amount': balancing_amount * orderbooks[balance_pair.split('/')[0]], 'pair': balance_pair, 'side': side})
                    # try:
                    #     telegram_bot.send_message(chat_id, f"Balancing order placed:\nPair: {balance_pair} | Side: {side}\nAmount: {round(balancing_amount, 8)} {balance_pair.split('/')[0]}\nAsset amount {round(balancing_amount * balance_orderbook['asks'][0][0], 8)} {balance_pair.split('/')[1]}\nUSDN order amount: {round(balancing_amount * orderbooks[balance_pair.split('/')[0]], 2)}\nOrder price: {price}")
                    # except:
                    #     telegram_bot.send_message(chat_id, f"Balancing order placed:\nPair: {balance_pair} | Side: {side}\nAmount: {round(balancing_amount, 8)} {balance_pair.split('/')[0]}\nAsset amount {round(balancing_amount * balance_orderbook['asks'][0][0], 8)} {balance_pair.split('/')[1]}\nUSDN order amount: {round(balancing_amount * orderbooks[balance_pair.split('/')[0]], 2)}\nOrder price: {price}")
                    # blocked_coins = balancing(pairs, coins, orderbooks, average_balance, blocked_coins)
                    # return blocked_coins


    # def autobalance(coins, amounts, orderbooks, pairs):
    #     average_balance = 0
    #     amounts_diffs = {}
    #     for coin in coins:
    #         average_balance += amounts[coin] * orderbooks[coin]
    #     average_balance = average_balance / len(coins)
    #     for coin in coins:
    #         amounts_diffs.update({coin: amounts[coin] * orderbooks[coin] - average_balance})
    #     orders = fetch_open_orders()
    #     orders = cancel_balancing(orders)
    #     # for order in orders:
    #     #     if order['side'] == 'buy':
    #     #         amounts_diffs[order['symbol'].split('/')[0]] += order['remaining'] * orderbooks[order['symbol'].split('/')[0]]
    #     #         amounts_diffs[order['symbol'].split('/')[1]] -= order['remaining'] * orderbooks[order['symbol'].split('/')[1]]
    #     #     else:
    #     #         amounts_diffs[order['symbol'].split('/')[0]] -= order['remaining'] * orderbooks[order['symbol'].split('/')[0]]
    #     #         amounts_diffs[order['symbol'].split('/')[1]] += order['remaining'] * orderbooks[order['symbol'].split('/')[1]]
    #     blocked_coins = []
    #     blocked_coins = balancing(pairs, amounts_diffs, orderbooks, average_balance, blocked_coins)
    #     return blocked_coins


timex_client = timex.WsClientTimex(cp["TIMEX"]["api_key"], cp["TIMEX"]["api_secret"])
bot = TriangleBot(timex_client)
bot.run()

