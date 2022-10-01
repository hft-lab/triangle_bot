import requests
import time
import ccxt
import json
import datetime
from multiprocessing import Process, Pipe
import telebot
import os
import base64
import axolotl_curve25519 as eddsar
from ccxt.static_dependencies import ecdsa
import re
import random
import sqlite3
import sys, os

offset = datetime.timedelta(hours=3)
datetime.timezone(offset, name='МСК')

bot = ccxt.wavesexchange({
})

data = bot.load_markets()

chat_id = -527608235
TOKEN = '1428903574:AAF3_l5cpFLBInw9maKHMwbTh5gv17YY44U'
telegram_bot = telebot.TeleBot(TOKEN)


def sql_create_orders_table():
    connect = sqlite3.connect('deals.db')
    cursor = connect.cursor()
    cursor.execute("""CREATE TABLE IF NOT EXISTS profit_deals (
    order_num INTEGER PRIMARY KEY AUTOINCREMENT,
    order_place_date TEXT,
    order_execute_date TEXT,
    triangle VARCHAR,
    maker_pair VARCHAR,
    maker_side TEXT,
    execute_percent TEXT,
    maker_order_price REAL,
    maker_order_position REAL,
    maker_coin_amount REAL,
    maker_deal_result REAL,
    maker_result VARCHAR,
    taker_pair_1 VARCHAR,
    taker_1_side TEXT,
    taker_1_order_price REAL,
    taker_1_depth REAL,
    taker_1_coin_amount REAL,
    taker_1_deal_result REAL,
    taker_1_result VARCHAR,
    taker_1_status TEXT,
    taker_1_time TEXT,
    taker_1_id VARCHAR,
    taker_pair_2 VARCHAR,
    taker_2_side TEXT,
    taker_2_order_price REAL,
    taker_2_depth REAL,
    taker_2_coin_amount REAL,
    taker_2_deal_result REAL,
    taker_2_result VARCHAR,
    taker_2_status TEXT,
    taker_2_time TEXT,
    taker_2_id VARCHAR,
    deal_result_perc REAL,
    deal_result_abs REAL,
    profit_coin TEXT,
    profit_USDN REAL,
    order_hang_time REAL
    );""")
    connect.commit()
    cursor.close()
    connect.close()

sql_create_orders_table()

def base_update(to_base):
    connect = sqlite3.connect('deals.db')
    cursor = connect.cursor()
    sql = f"""INSERT INTO profit_deals (
    order_place_date,
    order_execute_date,
    triangle,
    maker_pair,
    maker_side,
    execute_percent,
    maker_order_price,
    maker_order_position,
    maker_coin_amount,
    maker_deal_result,
    maker_result,
    taker_pair_1,
    taker_1_side,
    taker_1_order_price,
    taker_1_depth,
    taker_1_coin_amount,
    taker_1_deal_result,
    taker_1_result,
    taker_1_id,
    taker_pair_2,
    taker_2_side,
    taker_2_order_price,
    taker_2_depth,
    taker_2_coin_amount,
    taker_2_deal_result,
    taker_2_result,
    taker_2_id,
    deal_result_perc,
    deal_result_abs,
    profit_coin,
    profit_USDN,
    order_hang_time)
    VALUES ('{to_base["order_place_date"].split('.')[0]}',
    '{to_base["order_execute_date"]}', 
    '{to_base["triangle"]}', 
    '{to_base["maker_pair"]}', 
    '{to_base["maker_side"]}',
    {to_base["execute_percent"]},
    {to_base["maker_order_price"]},
    {to_base["maker_order_position"]},
    {to_base["maker_coin_amount"]},
    {to_base["maker_deal_result"]},
    '{to_base["maker_result"]}',
    '{to_base["taker_pair_1"]}',
    '{to_base["taker_1_side"]}',
    {to_base["taker_1_order_price"]},
    {to_base["taker_1_depth"]},
    {to_base["taker_1_coin_amount"]},
    {to_base["taker_1_deal_result"]},
    '{to_base["taker_1_result"]}',
    '{to_base['taker_1_id']}',
    '{to_base["taker_pair_2"]}',
    '{to_base["taker_2_side"]}',
    {to_base["taker_2_order_price"]},
    {to_base["taker_2_depth"]},
    {to_base["taker_2_coin_amount"]},
    {to_base["taker_2_deal_result"]},
    '{to_base["taker_2_result"]}',
    '{to_base['taker_2_id']}',
    {to_base["deal_result_perc"]},
    {to_base["deal_result_abs"]},
    '{to_base["profit_coin"]}',
    {to_base["profit_USDN"]},
    {to_base["order_hang_time"]}
    )"""
    try:
        cursor.execute(rf"{sql}")
    except Exception as e:
        pass
        telegram_bot.send_message(chat_id, f"DB error {e}\nData {sql}")
    connect.commit()
    cursor.close()
    connect.close()



def sql_create_partial_orders_table():
    connect = sqlite3.connect('deals.db')
    cursor = connect.cursor()
    cursor.execute("""CREATE TABLE IF NOT EXISTS partial_deals (
    order_num INTEGER PRIMARY KEY AUTOINCREMENT,
    order_place_date VARCHAR,
    order_execute_date VARCHAR,
    triangle VARCHAR,
    maker_pair TEXT,
    maker_side TEXT,
    execute_percent REAL,
    maker_order_price REAL,
    maker_order_position REAL,
    maker_coin_amount REAL,
    maker_deal_result REAL,
    maker_result VARCHAR,
    taker_pair_1 TEXT,
    taker_1_side TEXT,
    taker_1_order_price REAL,
    taker_1_depth REAL,
    taker_1_coin_amount REAL,
    taker_1_deal_result REAL,
    taker_1_result VARCHAR,
    taker_pair_2 TEXT,
    taker_2_side TEXT,
    taker_2_order_price REAL,
    taker_2_depth REAL,
    taker_2_coin_amount REAL,
    taker_2_deal_result REAL,
    taker_2_result VARCHAR,
    deal_result_perc REAL,
    deal_result_abs REAL,
    profit_coin TEXT,
    profit_USDN REAL,
    order_hang_time REAL
    );""")
    connect.commit()
    cursor.close()
    connect.close()

sql_create_partial_orders_table()

def base_partial_update(to_base):
    connect = sqlite3.connect('deals.db')
    cursor = connect.cursor()
    sql = f"""INSERT INTO partial_deals (
    order_place_date,
    order_execute_date,
    triangle,
    maker_pair,
    maker_side,
    execute_percent,
    maker_order_price,
    maker_order_position,
    maker_coin_amount,
    maker_deal_result,
    maker_result,
    taker_pair_1,
    taker_1_side,
    taker_1_order_price,
    taker_1_depth,
    taker_1_coin_amount,
    taker_1_deal_result,
    taker_1_result,
    taker_pair_2,
    taker_2_side,
    taker_2_order_price,
    taker_2_depth,
    taker_2_coin_amount,
    taker_2_deal_result,
    taker_2_result,
    deal_result_perc,
    deal_result_abs,
    profit_coin,
    profit_USDN,
    order_hang_time)
    VALUES ('{to_base["order_place_date"].split('.')[0]}',
    '{to_base["order_execute_date"]}', 
    '{to_base["triangle"]}', 
    '{to_base["maker_pair"]}', 
    '{to_base["maker_side"]}',
    {to_base["execute_percent"]},
    {to_base["maker_order_price"]},
    {to_base["maker_order_position"]},
    {to_base["maker_coin_amount"]},
    {to_base["maker_deal_result"]},
    '{to_base["maker_result"]}',
    '{to_base["taker_pair_1"]}',
    '{to_base["taker_1_side"]}',
    {to_base["taker_1_order_price"]},
    {to_base["taker_1_depth"]},
    {to_base["taker_1_coin_amount"]},
    {to_base["taker_1_deal_result"]},
    '{to_base["taker_1_result"]}',
    '{to_base["taker_pair_2"]}',
    '{to_base["taker_2_side"]}',
    {to_base["taker_2_order_price"]},
    {to_base["taker_2_depth"]},
    {to_base["taker_2_coin_amount"]},
    {to_base["taker_2_deal_result"]},
    '{to_base["taker_2_result"]}',
    {to_base["deal_result_perc"]},
    {to_base["deal_result_abs"]},
    '{to_base["profit_coin"]}',
    {to_base["profit_USDN"]},
    {to_base["order_hang_time"]}
    )"""
    try:
        cursor.execute(rf"{sql}")
    except Exception as e:
        pass
        telegram_bot.send_message(chat_id, f"DB partial error {e}\nData {sql}")
    connect.commit()
    cursor.close()
    connect.close()


def balancing(pairs, coins, orderbooks, average_balance, blocked_coins):
    for first_coin, first_amount in coins.items():
        for second_coin, second_amount in coins.items():
            if first_coin == second_coin:
                continue
            if average_balance * 0.05 > abs(first_amount) or average_balance * 0.05 > abs(second_amount):
                continue
            if first_amount > 0 and second_amount < 0 or first_amount < 0 and second_amount > 0:
                for pair in pairs:
                    if first_coin in pair and second_coin in pair:
                        balance_pair = pair
                if first_amount > 0:
                    sell_coin, buy_coin, sell_amount, buy_amount = first_coin, second_coin, first_amount, second_amount
                else:
                    sell_coin, buy_coin, sell_amount, buy_amount = second_coin, first_coin, second_amount, first_amount
                balancing_amount_usd = abs(buy_amount) if abs(buy_amount) < abs(sell_amount) else abs(sell_amount)
                coins[sell_coin] -= balancing_amount_usd
                coins[buy_coin] += balancing_amount_usd
                if sell_coin == balance_pair.split('/')[0]:
                    side, orderbook = 'sell', 'asks'
                else:
                    side, orderbook = 'buy', 'bids'
                balance_orderbook = fetch_orderbook(balance_pair, 1)
                balancing_amount = balancing_amount_usd / orderbooks[balance_pair.split('/')[0]]
                ticksize = 1 / 10 ** len(str(balance_orderbook['asks'][0][0]).split('.')[1])
                if side == 'buy':
                    price = balance_orderbook['bids'][0][0] + ticksize
                else:
                    price = balance_orderbook['asks'][0][0] - ticksize
                place_order({'pair': balance_pair, 'side': side, 'amount': balancing_amount, 'price': price})
                if side == 'sell':
                    blocked_coins.append({'coin': balance_pair.split('/')[0], 'amount': balancing_amount * orderbooks[balance_pair.split('/')[0]], 'pair': balance_pair, 'side': side})
                else:
                    blocked_coins.append({'coin': balance_pair.split('/')[1], 'amount': balancing_amount * orderbooks[balance_pair.split('/')[0]], 'pair': balance_pair, 'side': side})
                # try:
                #     telegram_bot.send_message(chat_id, f"Balancing order placed:\nPair: {balance_pair} | Side: {side}\nAmount: {round(balancing_amount, 8)} {balance_pair.split('/')[0]}\nAsset amount {round(balancing_amount * balance_orderbook['asks'][0][0], 8)} {balance_pair.split('/')[1]}\nUSDN order amount: {round(balancing_amount * orderbooks[balance_pair.split('/')[0]], 2)}\nOrder price: {price}")
                # except:
                #     telegram_bot.send_message(chat_id, f"Balancing order placed:\nPair: {balance_pair} | Side: {side}\nAmount: {round(balancing_amount, 8)} {balance_pair.split('/')[0]}\nAsset amount {round(balancing_amount * balance_orderbook['asks'][0][0], 8)} {balance_pair.split('/')[1]}\nUSDN order amount: {round(balancing_amount * orderbooks[balance_pair.split('/')[0]], 2)}\nOrder price: {price}")
                blocked_coins = balancing(pairs, coins, orderbooks, average_balance, blocked_coins)
                return blocked_coins

def cancel_balancing(orders):
    if len(orders):
        for order in orders:
            # bidAsk = 'asks' if order['side'] == 'sell' else 'bids'
            # order_info = fetch_orderbook(order['symbol'], 50)
            # position = 'over 30'
            # for pos in order_info[bidAsk]:
            #     if pos[0] == order['price']:
            #         position = order_info[bidAsk].index(pos) + 1
            #         if position > 1:
            cancel_order(order['id'])
            # time.sleep(5)
            # orders = fetch_open_orders()
                # if position == 'over 30':
                #     cancel_order(order['id'])
                #     time.sleep(5)
                #     orders = fetch_open_orders()
    return orders


def autobalance(coins, amounts, orderbooks, pairs):
    average_balance = 0
    amounts_diffs = {}
    for coin in coins:
        average_balance += amounts[coin] * orderbooks[coin]
    average_balance = average_balance / len(coins)
    for coin in coins:
        amounts_diffs.update({coin: amounts[coin] * orderbooks[coin] - average_balance})
    orders = fetch_open_orders()
    orders = cancel_balancing(orders)
    # for order in orders:
    #     if order['side'] == 'buy':
    #         amounts_diffs[order['symbol'].split('/')[0]] += order['remaining'] * orderbooks[order['symbol'].split('/')[0]]
    #         amounts_diffs[order['symbol'].split('/')[1]] -= order['remaining'] * orderbooks[order['symbol'].split('/')[1]]
    #     else:
    #         amounts_diffs[order['symbol'].split('/')[0]] -= order['remaining'] * orderbooks[order['symbol'].split('/')[0]]
    #         amounts_diffs[order['symbol'].split('/')[1]] += order['remaining'] * orderbooks[order['symbol'].split('/')[1]]
    blocked_coins = []
    blocked_coins = balancing(pairs, amounts_diffs, orderbooks, average_balance, blocked_coins)
    return blocked_coins



def start_balance_message(amounts_start, orderbooks, coins, balance):
    amounts_total_start = {'WAVES': 219.32983894,
                            'USDN': 1929.357886,
                            'BTC': 0.0338665,
                            'USDT': 0,
                            'LTC': 0,
                            'ETH': 0}
    # amounts_total_start = {'WAVES': 100.669864,
    #                     'USDN': 1249.548166,
    #                     'BTC': 0.017559,
    #                     'USDT': 1010.080675,
    #                     'LTC': 4.161933,
    #                     'ETH': 0.527587}
    message_now = f'Current\n'
    message_start = f'Start\n'
    now_balance = 0
    total_start_balance = 0
    bot_start_balance = 0
    for coin in coins:
        string_len = 6 - len(coin)
        if coin == 'USDN':
            now_balance += balance['total'][coin]
            total_start_balance += amounts_total_start[coin]
            bot_start_balance += amounts_start[coin]
            start_len = 11 - len(str(round(amounts_start[coin], 6)))
            total_len = 11 - len(str(round(balance['total'][coin], 6)))
            message_start += f"{coin}" + ' ' * string_len + f"{round(amounts_start[coin], 6)}" + ' ' * start_len + f"({round(amounts_start[coin], 2)})\n"
            message_now += f"{coin}" + ' ' * string_len + f"{round(balance['total'][coin], 6)}" + ' ' * total_len + f"({round(balance['total'][coin], 2)})\n"
            continue
        now_balance += balance['total'][coin] * orderbooks[coin]
        total_start_balance += amounts_total_start[coin] * orderbooks[coin]
        bot_start_balance += amounts_start[coin] * orderbooks[coin]
        start_len = 11 - len(str(round(amounts_start[coin], 6)))
        total_len = 11 - len(str(round(balance['total'][coin], 6)))
        message_start += f"{coin}" + ' ' * string_len + f"{round(amounts_start[coin], 6)}" + ' ' * start_len + f"({round(amounts_start[coin] * orderbooks[coin], 2)})\n"
        message_now += f"{coin}" + ' ' * string_len + f"{round(balance['total'][coin], 6)}" + ' ' * total_len + f"({round(balance['total'][coin] * orderbooks[coin], 2)})\n"
    message_start += 8 * '- ' + ' ' + f"{round(bot_start_balance, 2)}"
    len_now = 15 - len(f'Project: {round(bot_start_balance - total_start_balance, 2)}')
    message_start += f'\nProject: {round(bot_start_balance - total_start_balance, 2)}' + ' ' * len_now + f'({round((bot_start_balance - total_start_balance) / total_start_balance * 100, 2)}%)'
    message_start += f"\nProject delta: {round((now_balance - total_start_balance) - (bot_start_balance - total_start_balance), 2)}\n"
    len_now = 15 - len(f'Project: {round(now_balance - total_start_balance, 2)}')
    message_now += 8 * '- ' + ' ' + f"{round(now_balance, 2)}\n"
    message_now += f'Project: {round(now_balance - total_start_balance, 2)}' + ' ' * len_now + f'({round((now_balance - total_start_balance) / total_start_balance * 100, 2)}%)\n\n'

    message = message_now + message_start
    return message, now_balance, total_start_balance, bot_start_balance


def check_balance(amounts_start = None, balancing = False, pairs = None, project_start_balance = None):
    balance = fetch_balance()
    coins = list(set(balance['total']))
    orderbooks = {'USDN': 1}
    amounts = {}
    for coin in coins:
        if coin == 'USDN':
            amounts.update({coin: balance['total']['USDN']})
            continue
        orderbooks.update({coin: fetch_orderbook(coin + '/USDN', 1)['asks'][0][0]})
        amounts.update({coin: balance['total'][coin]})
    if not amounts_start:
        amounts_start = amounts
    final_message, now_balance, total_start_balance, bot_start_balance = start_balance_message(amounts_start, orderbooks, coins, balance)
    len_session = 15 - len(f'Session: {round(now_balance - bot_start_balance, 2)}')
    # len_now = 15 - len(f'Project: {round(now_balance - total_start_balance, 2)}')
    final_message += f'\nProfit (USDN)\nSession: {round(now_balance - bot_start_balance, 2)}' + ' ' * len_session +  f'({round((now_balance - bot_start_balance) / bot_start_balance * 100, 2)}%)\n'
    # final_message += f'Project: {round(now_balance - total_start_balance, 2)}' + ' ' * len_now + f'({round((now_balance - total_start_balance) / total_start_balance * 100, 2)}%)\n'
    if project_start_balance:
        final_message += f"Prj chg: {round(now_balance - total_start_balance - project_start_balance, 2)}({round((now_balance - total_start_balance - project_start_balance) / (now_balance - total_start_balance) * 100, 4)}%)"
    if balancing:
        blocked_coins = autobalance(coins, amounts, orderbooks, pairs)
    # message = find_open_orders()
    # final_message += message
    if not balancing:
        try:
            telegram_bot.send_message(chat_id, '<pre>' + final_message + '</pre>', parse_mode = 'HTML')
        except:
            telegram_bot.send_message(chat_id, '<pre>' + final_message + '</pre>', parse_mode = 'HTML')
    if balancing:
        return blocked_coins
    else:    
        return [amounts, round(now_balance - total_start_balance, 2)]

def open_order(process_num, child_connection):
    while True:
        time.sleep(0.0001)
        if child_connection.poll():
            order_chain = child_connection.recv()
            # order_result = False
            # start_time = time.time()
            # start_datetime = datetime.datetime.now()
            time.sleep(0.02)
            order_result = place_order(order_chain)
            count = 0
            while not order_result['success']:
                if count == 50:
                    try:
                        telegram_bot.send_message(chat_id, f"Sub process {process_num}\nOrder {order_chain['pair']} side {order_chain['side']}\nWasnt placed, responce:\n{order_result}")
                    except:
                        telegram_bot.send_message(chat_id, f"Sub process {process_num}\nOrder {order_chain['pair']} side {order_chain['side']}\nWasnt placed, responce:\n{order_result}")
                    break
                order_result = place_order(order_chain)
                count += 1
            try:
                child_connection.send(order_result['message']['id'])
            except:
                child_connection.send(None)
            # end_time = time.time() - start_time
            # end_datetime = datetime.datetime.now()
            # try:
            #     telegram_bot.send_message(chat_id, f"Sub process {process_num}\nOrder {order_chain['pair']} side {order_chain['side']} filled.\nFinal amount {order_chain['amount']}, final price {order_chain['price']}\nExecute time {end_time}\nProcess started at {start_datetime}\nEnded at {end_datetime}\nOrder result: {order_result['success']}")
            # except:
            #     telegram_bot.send_message(chat_id, f"Sub process {process_num}\nOrder {order_chain['pair']} side {order_chain['side']} filled.\nFinal amount {order_chain['amount']}, final price {order_chain['price']}\nExecute time {end_time}\nProcess started at {start_datetime}\nEnded at {end_datetime}\nOrder result: {order_result['success']}")

def change_max_order_amount(blocked_coins, triangles_coins):    
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


def triangle_trading_waves(parent_connection_1, parent_connection_5, parent_connection_4, data):
    try:
        all_orders = bot.fetchOpenOrders()
        for i in all_orders:
            bot.cancelOrder(i['info']['id'])
    except:
        pass
    balance = fetch_balance()
    coins = list(set(balance['total']))
    pairs = set(data)
    orders_num = 9
    triangles_coins = []
    coin_sets = []
    triangle_sets = []
    for coin_1 in coins:
        for coin_2 in coins:
            for coin_3 in coins:
                if coin_1 == coin_2 or coin_1 == coin_3 or coin_2 == coin_3:
                    continue
                if set([coin_1, coin_2, coin_3]) not in coin_sets:
                    coin_sets.append(set([coin_1, coin_2, coin_3]))
                    for pair_1 in pairs:
                        for pair_2 in pairs:
                            for pair_3 in pairs:
                                if pair_1 == pair_2 or pair_1 == pair_3 or pair_2 == pair_3:
                                    continue
                                if set([coin for pair in [x.split('/') for x in [pair_1, pair_2, pair_3]] for coin in pair ]) == set([coin_1, coin_2, coin_3]):
                                    if set([pair_1, pair_2, pair_3]) not in triangle_sets:
                                        triangle_sets.append(set([pair_1, pair_2, pair_3]))
                                        triangles_coins.append({'coins': [coin_1, coin_2, coin_3], 'pairs': [pair_1, pair_2, pair_3]})
    try:
        telegram_bot.send_message(chat_id, f'Bot big_bot_1.14_non_sockets_full_stack_spread (9 ордеров - пакетное размещение в спреды - 3 процесса, сортировка по volume(пол стека) started.')
    except:
        telegram_bot.send_message(chat_id, f'Bot big_bot_1.14_non_sockets_full_stack_spread (9 ордеров - пакетное размещение в спреды - 3 процесса, сортировка по volume(пол стека) started.' + ' EXCEPT')
    depth = 20
    depth_maker = 2
    first_pos = 0
    profit_abs_last = {'coin': None, 'profit_abs': None}
    i = 0
    all_pairs = [pair for triangle in triangle_sets for pair in triangle]
    last_orders_amounts = []
    last_orders_prices = []
    last_sending = ['None', 'None', 'None']
    while True:
        try:
            # print('Parser')
            if parent_connection_1.poll():
                start_amounts = parent_connection_1.recv()
                    # dump = None
            #     dump = parent_connection_1.recv()
            # while parent_connection_4.poll():
            #     dump = parent_connection_4.recv()
            # while parent_connection_5.poll():
            #     dump = parent_connection_5.recv()
            #     if dump == 'Pause':
                    # print('Send 3 None')
                    # if last_sending[2] != 'None':
                    #     last_sending[2] = 'None'
                    #     parent_connection_5.send('None')
                    # if last_sending[1] != 'None':
                    #     last_sending[1] = 'None'
                    #     parent_connection_4.send('None')
                    # if last_sending[0] != 'None':
                    #     last_sending[0] = 'None'
                    #     parent_connection_1.send('None')
                    # print('Balancing started')
                #     time.sleep(2)
                #     dump = None
                #     blocked_coins = check_balance(amounts_start = start_amounts, balancing = True, pairs = all_pairs)
                # else:
                #     start_amounts = dump
                #     dump = None

            if not i % 500:
                # print('Send 3 None')
                # if last_sending[2] != 'None':
                #     last_sending[2] = 'None'
                #     parent_connection_5.send('None')
                # if last_sending[1] != 'None':
                #     last_sending[1] = 'None'
                #     parent_connection_4.send('None')
                # if last_sending[0] != 'None':
                #     last_sending[0] = 'None'
                #     parent_connection_1.send('None')
                blocked_coins = check_balance(amounts_start = start_amounts[0], balancing = True, pairs = all_pairs, project_start_balance = start_amounts[1])
                data = bot.load_markets()
                volumes = {}
                for pair in all_pairs:
                    volumes.update({pair: float(data[pair]['info']['24h_volume'])})
            if not i % 5000:    
                doc = open('deals.db', 'rb')
                telegram_bot.send_document(chat_id, doc)
                doc.close()
            parse_start = time.time()

            MAX_ORDER_AMOUNTS, convert_orderbooks, orderbooks = change_max_order_amount(blocked_coins, triangles_coins)
            for pair in all_pairs:
                if pair.split('/')[1] != 'USDN':
                    orderbooks.update({pair: my_fetch_order_book_waves(pair, 20)})
            count_time = time.time()
            triangles = []
            comission = 0.009 * orderbooks['WAVES/USDN']['asks'][0][0]
            for triangle in triangles_coins:
                for coin_1 in triangle['coins']:
                    for pair_1 in triangle['pairs']:
                        for pair_2 in triangle['pairs']:
                            for pair_3 in triangle['pairs']:
                                if pair_1 == pair_2 or pair_1 == pair_3 or pair_2 == pair_3:
                                    continue
                                if coin_1 in pair_1 and coin_1 in pair_3:
                                    coins_chain = {}
                                    # Формирование первого главного коина в словаре
                                    if coin_1 == pair_1.split('/')[0]:
                                        coins_chain.update({'coin_1': {'coin': coin_1, 'pair': pair_1, 'side': 'sell', 'orderbook_side': 'asks', 'orderbook': orderbooks[pair_1]['asks'], 'price': orderbooks[pair_1]['asks'][0][0]}})
                                        coin_2 = pair_1.split('/')[1]
                                    else:
                                        coins_chain.update({'coin_1': {'coin': coin_1, 'pair': pair_1, 'side': 'buy', 'orderbook_side': 'bids', 'orderbook': orderbooks[pair_1]['bids'], 'price': orderbooks[pair_1]['bids'][0][0]}})
                                        coin_2 = pair_1.split('/')[0]
                                    coin_3 = [x for x in triangle['coins'] if x not in [coin_1, coin_2]][0]
                                    MAX_ORDER_AMOUNT = [x['max_amount'] for x in MAX_ORDER_AMOUNTS if coin_1 in x['coins'] and coin_3 in x['coins']][0]
                                    # Формирование 2 и 3 коина в словаре
                                    for num, coin, pair in [(2, coin_2, pair_2), (3, coin_3, pair_3)]:
                                        if coin == pair.split('/')[0]:
                                            side, orderbook_side = 'sell', 'bids'
                                        else:
                                            side, orderbook_side = 'buy', 'asks'
                                        coins_chain.update({'coin_' + str(num): {'coin': coin, 'pair': pair, 'side': side, 'orderbook_side': orderbook_side, 'orderbook': orderbooks[pair][orderbook_side]}})
                                    # просчет размеров лота в глубину
                                    depth_count_2 = []
                                    depth_count_3 = []
                                    
                                    for position in range(depth):
                                        try:
                                            amount = sum([coins_chain['coin_' + str(2)]['orderbook'][x][1] for x in range(position) if coins_chain['coin_' + str(2)]['orderbook'][x][1] not in last_orders_amounts])
                                            usdAmount = sum([coins_chain['coin_' + str(2)]['orderbook'][x][1] for x in range(position) if coins_chain['coin_' + str(2)]['orderbook'][x][1] not in last_orders_amounts]) * convert_orderbooks[coins_chain['coin_' + str(2)]['pair'].split('/')[0]]
                                            price = coins_chain['coin_' + str(2)]['orderbook'][position][0]
                                        except:
                                            break
                                        if price in last_orders_prices or amount == 0:
                                            continue
                                        depth_chain = {'depth': position,'price': price, 'amount': amount, 'usdAmount': usdAmount}
                                        depth_chain['usdAmount'] = MAX_ORDER_AMOUNT if depth_chain['usdAmount'] > MAX_ORDER_AMOUNT else depth_chain['usdAmount']
                                        depth_count_2.append(depth_chain)
                                        if depth_chain['usdAmount'] == MAX_ORDER_AMOUNT:
                                            if position != depth - 1:
                                                break
                                    
                                    for position in range(depth):
                                        try:
                                            amount = sum([coins_chain['coin_' + str(3)]['orderbook'][x][1] for x in range(position) if coins_chain['coin_' + str(3)]['orderbook'][x][1] not in last_orders_amounts])
                                            usdAmount = sum([coins_chain['coin_' + str(3)]['orderbook'][x][1] for x in range(position) if coins_chain['coin_' + str(3)]['orderbook'][x][1] not in last_orders_amounts]) * convert_orderbooks[coins_chain['coin_' + str(3)]['pair'].split('/')[0]]
                                            price = coins_chain['coin_' + str(3)]['orderbook'][position][0]
                                        except:
                                            break
                                        if price in last_orders_prices or amount == 0:
                                            continue
                                        depth_chain = {'depth': position,'price': price, 'amount': amount, 'usdAmount': usdAmount}
                                        depth_chain['usdAmount'] = MAX_ORDER_AMOUNT if depth_chain['usdAmount'] > MAX_ORDER_AMOUNT else depth_chain['usdAmount']
                                        depth_count_3.append(depth_chain)
                                        if depth_chain['usdAmount'] == MAX_ORDER_AMOUNT:
                                            if position != depth - 1:
                                                break
                                    
                                    for coin_2 in depth_count_2:
                                        for coin_3 in depth_count_3:
                                            min_amount = coin_2['usdAmount'] if coin_2['usdAmount'] < coin_3['usdAmount'] else coin_3['usdAmount']
                                            if min_amount <= 0:
                                                continue
                                            initial_amount = (min_amount + comission) / convert_orderbooks[coins_chain['coin_1']['coin']]
                                            profit = random.choice([x * 0.0005 for x in range(2, 11)])
                                            # profit = 0.001
                                            end_amount = (1 + profit) * initial_amount
                                            if coins_chain['coin_2']['side'] == 'sell':
                                                convert_price_2 =  coin_2['price']
                                            else:
                                                convert_price_2 = 1 / coin_2['price']
                                            if coins_chain['coin_3']['side'] == 'sell':
                                                convert_price_3 =  coin_3['price']
                                            else:
                                                convert_price_3 = 1 / coin_3['price']
                                            # CONVERTATIONS
                                            if coins_chain['coin_3']['side'] == 'buy':
                                                amount_3 = end_amount
                                            else:
                                                amount_3 = end_amount / convert_price_3
                                            if coins_chain['coin_3']['side'] == 'buy' and coins_chain['coin_2']['side'] == 'buy':
                                                amount_2 = amount_3 / convert_price_3  
                                            if coins_chain['coin_3']['side'] == 'buy' and coins_chain['coin_2']['side'] == 'sell':
                                                amount_2 = amount_3 / convert_price_2 / convert_price_3                                            
                                            if coins_chain['coin_3']['side'] == 'sell' and coins_chain['coin_2']['side'] == 'buy':
                                                amount_2 = amount_3
                                            if coins_chain['coin_3']['side'] == 'sell' and coins_chain['coin_2']['side'] == 'sell':
                                                amount_2 = amount_3 / convert_price_2
                                            if coins_chain['coin_2']['side'] == 'buy' and coins_chain['coin_1']['side'] == 'buy':
                                                amount_1 = amount_2 / convert_price_2
                                                convert_price_1 = amount_1 / min_amount * convert_orderbooks[pair_1.split('/')[1]]
                                                main_price = 1 / convert_price_1
                                            if coins_chain['coin_2']['side'] == 'sell' and coins_chain['coin_1']['side'] == 'buy':
                                                amount_1 = amount_2
                                                convert_price_1 = amount_1 / min_amount * convert_orderbooks[pair_1.split('/')[1]]
                                                main_price = 1 / convert_price_1
                                            if coins_chain['coin_2']['side'] == 'sell' and coins_chain['coin_1']['side'] == 'sell':
                                                amount_1 = min_amount / convert_orderbooks[pair_1.split('/')[0]]
                                                main_price = amount_2 / amount_1
                                            if coins_chain['coin_2']['side'] == 'buy' and coins_chain['coin_1']['side'] == 'sell':
                                                amount_1 = min_amount / convert_orderbooks[pair_1.split('/')[0]]
                                                main_price = amount_2 / amount_1 / convert_price_2
                                            if coins_chain['coin_1']['side'] == 'sell':
                                                spread = (orderbooks[coins_chain['coin_1']['pair']]['asks'][0][0] - orderbooks[coins_chain['coin_1']['pair']]['bids'][0][0]) / orderbooks[coins_chain['coin_1']['pair']]['bids'][0][0] * 100
                                                position = (orderbooks[coins_chain['coin_1']['pair']]['asks'][0][0] - main_price) / (orderbooks[coins_chain['coin_1']['pair']]['asks'][0][0] - orderbooks[coins_chain['coin_1']['pair']]['bids'][0][0]) * 100
                                                if main_price > coins_chain['coin_1']['orderbook'][0][0]:
                                                    continue
                                            else:
                                                spread = (orderbooks[coins_chain['coin_1']['pair']]['asks'][0][0] - orderbooks[coins_chain['coin_1']['pair']]['bids'][0][0]) / orderbooks[coins_chain['coin_1']['pair']]['asks'][0][0] * 100
                                                position = (main_price - orderbooks[coins_chain['coin_1']['pair']]['bids'][0][0]) / (orderbooks[coins_chain['coin_1']['pair']]['asks'][0][0] - orderbooks[coins_chain['coin_1']['pair']]['bids'][0][0]) * 100
                                                if main_price < coins_chain['coin_1']['orderbook'][0][0]:
                                                    continue
                                            profit_abs = (end_amount - initial_amount) * convert_orderbooks[coins_chain['coin_1']['coin']]
                                            if coin_3 == profit_abs_last['coin']:
                                                if profit_abs < profit_abs_last['profit_abs']:
                                                    if coin_3 != depth_count_3[-1]:
                                                        break
                                            profit_abs_last = {'coin': coin_3, 'profit_abs': profit_abs}
                                            if profit_abs > 0.1:
                                                price_len = data[coins_chain['coin_1']['pair']]['precision']['price']
                                                amount_len = data[coins_chain['coin_1']['pair']]['precision']['amount']
                                                order_chain = [{'pair': coins_chain['coin_1']['pair'], 'side': coins_chain['coin_1']['side'], 'amount': round(amount_1, amount_len), 'price': round(main_price, price_len), 'spread': spread, 'position': position, 'main_coin': coin_1, 'last_coin': coin_3, 'convert_orderbooks': convert_orderbooks},
                                                {'pair': coins_chain['coin_2']['pair'], 'side': coins_chain['coin_2']['side'], 'amount': amount_2, 'price': coin_2['price'], 'depth': coin_2['depth']},
                                                {'pair': coins_chain['coin_3']['pair'], 'side': coins_chain['coin_3']['side'], 'amount': amount_3, 'price': coin_3['price'], "depth": coin_3['depth']}]
                                                triangles.append([order_chain, profit_abs])
            last_orders_amounts = []
            last_orders_prices = []
            # uniq = set([(x[0][0]['pair'], x[0][0]['side']) for x in triangles])
            # print(f"Parse {i}. Profit unique triangles {len(uniq)}")
            # uniq = set([(x[0][0]['pair'], x[0][0]['side']) for x in triangles])
            # print(f"Parse {i}. Profit unique triangles {len(uniq)}. Triangles:\n{uniq}")
            # print(f"Blocked coins: {blocked_coins}")
            i += 1
            # try:
            if len(triangles):
                orders = sort_triangles(triangles, MAX_ORDER_AMOUNTS, orders_num, convert_orderbooks, blocked_coins, volumes)
                # print(f"Len triangles after sorting: {len(orders)}")
            else:
                continue
            while len(orders) < orders_num:
                orders.append(False)
            with open('data_file.json', 'w') as file:
                json.dump(orders, file)
            check_orders_status()
            # time.sleep(0.5)
            for maker in orders:
                if maker:
                    last_orders_amounts.append(maker[0]['amount'])
                    last_orders_prices.append(maker[0]['price'])
        except Exception as e:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            try:
                telegram_bot.send_message(chat_id, f"Parser error {e}. Error on line {exc_tb.tb_lineno}")
            except:
                pass
        # except Exception as e:
        #     telegram_bot.send_message(chat_id, f'Parser error {e}')

            # print(f'Parse {i}. Parse time {time.time() - parse_start} Count time {time.time() - count_time} Orders: {len(orders)}')
        # except Exception as e:
        #     try:
        #         telegram_bot.send_message(chat_id, f"Parser crushed {e}")
        #     except:
        #         pass
        # 20 21 29 30 13 14 15 22 23 24

# 0    order_num INTEGER PRIMARY KEY AUTOINCREMENT,
# 1    order_place_date TEXT,
# 2    order_execute_date TEXT,
# 3    triangle VARCHAR,
# 4    maker_pair VARCHAR,
# 5    maker_side TEXT,
# 6    execute_percent TEXT,
# 7    maker_order_price REAL,
# 8    maker_order_position REAL,
# 9    maker_coin_amount REAL,
# 10    maker_deal_result REAL,
# 11    maker_result VARCHAR,
# 12    taker_pair_1 VARCHAR,
# 13    taker_1_side TEXT,
# 14    taker_1_order_price REAL,
# 15    taker_1_depth REAL,
# 16    taker_1_coin_amount REAL,
# 17    taker_1_deal_result REAL,
# 18    taker_1_result VARCHAR,
# 19    taker_1_status TEXT,
# 20    taker_1_time TEXT,
# 21    taker_1_id VARCHAR,
# 22    taker_pair_2 VARCHAR,
# 23    taker_2_side TEXT,
# 24    taker_2_order_price REAL,
# 25    taker_2_depth REAL,
# 26    taker_2_coin_amount REAL,
# 27    taker_2_deal_result REAL,
# 28    taker_2_result VARCHAR,
# 29    taker_2_status TEXT,
# 30    taker_2_time TEXT,
# 31    taker_2_id VARCHAR,
# 32    deal_result_perc REAL,
# 33    deal_result_abs REAL,
# 34    profit_coin TEXT,
# 35    profit_USDN REAL,
# 36    order_hang_time REAL

def check_orders_status():
    connect = sqlite3.connect('deals.db')
    cursor = connect.cursor()
    last = cursor.execute("SELECT * FROM profit_deals;").fetchall()
    for deal in last[::-1]:
        if not deal[21] and not deal[31]:
            break
        if not deal[19] and deal[21]:
            last_order = fetch_order(deal[12], deal[21])
            if not last_order:
                sql = f"""UPDATE profit_deals SET taker_1_status = ?, taker_1_time = ? WHERE order_num = ?;"""
                column_values = ('Not found', str(datetime.datetime.now(datetime.timezone(offset))).split('.')[0], deal[0])
                cursor.execute(sql, column_values)
                # print(f"Pair: {deal[12]}, Id: {deal[21]}")
                # continue
            if last_order:
                if last_order['filled'] / last_order['amount'] > 0.7:
                    sql = f"""UPDATE profit_deals SET taker_1_status = ?, taker_1_time = ? WHERE order_num = ?;"""
                    column_values = ('Done', str(datetime.datetime.now(datetime.timezone(offset))).split('.')[0], deal[0])
                    cursor.execute(sql, column_values)
                else:
                    if last_order['status'] == 'open':
                        pass
                    else:
                        sql = f"""UPDATE profit_deals SET taker_1_status = ?, taker_1_time = ? WHERE order_num = ?;"""
                        column_values = ('Canceled', str(datetime.datetime.now(datetime.timezone(offset))).split('.')[0], deal[0])
                        cursor.execute(sql, column_values)
        if not deal[29] and deal[31]:
            last_order = fetch_order(deal[22], deal[31])
            if not last_order:
                sql = f"""UPDATE profit_deals SET taker_2_status = ?, taker_2_time = ? WHERE order_num = ?;"""
                column_values = ('Not found', str(datetime.datetime.now(datetime.timezone(offset))).split('.')[0], deal[0])
                cursor.execute(sql, column_values)
                # print(f"Pair: {deal[21]}, Id: {deal[31]}")
                # continue
            if last_order:
                if last_order['filled'] / last_order['amount'] > 0.7:
                    sql = f"""UPDATE profit_deals SET taker_2_status = ?, taker_2_time = ? WHERE order_num = ?;"""
                    column_values = ('Done', str(datetime.datetime.now(datetime.timezone(offset))).split('.')[0], deal[0])
                    cursor.execute(sql, column_values)
                else:
                    if last_order['status'] == 'open':
                        pass
                    else:
                        sql = f"""UPDATE profit_deals SET taker_2_status = ?, taker_2_time = ? WHERE order_num = ?;"""
                        column_values = ('Canceled', str(datetime.datetime.now(datetime.timezone(offset))).split('.')[0], deal[0])
                        cursor.execute(sql, column_values)              
    connect.commit()
    cursor.close()
    connect.close()


def sort_triangles(triangles, MAX_ORDER_AMOUNTS, orders_num, convert_orderbooks, blocked_coins, volumes):
    orders = []
    repeat = False
    passed = False
    for circle in range(orders_num):
        if len(triangles):
            best_one = triangles[0]
            for triangle in triangles:
                if triangle[0][0]['amount'] < 0 or triangle[0][1]['amount'] < 0 or triangle[0][2]['amount'] < 0:
                    triangles.remove(triangle)
                    continue
                if len(orders):    
                    for last_order in orders:
                        if triangle[0][0]['pair'] == last_order[0]['pair']:
                            if triangle[0][0]['side'] == last_order[0]['side']:
                                repeat = True
                if blocked_coins:
                    for block_pair in blocked_coins:
                        if triangle[0][0]['pair'] == block_pair[0]['pair']:
                            if triangle[0][0]['side'] == block_pair[0]['side']:
                                repeat = True
                if repeat:
                    repeat = False
                    # print('repeat')
                    triangles.remove(triangle)
                    continue
                pair = triangle[0][0]['pair']
                main_coin = triangle[0][0]['pair'].split('/')[0]
                last_pair = best_one[0][0]['pair']
                last_main_coin = best_one[0][0]['pair'].split('/')[0]
                # if volumes[pair] * convert_orderbooks[main_coin] > volumes[last_pair] * convert_orderbooks[last_main_coin]:
                for max_amount in MAX_ORDER_AMOUNTS:
                    if triangle[0][0]['main_coin'] in max_amount['coins']:
                        if max_amount['max_amount'] > triangle[0][0]['amount'] * convert_orderbooks[triangle[0][0]['pair'].split('/')[0]]:
                            best_one = triangle
                            passed = True

            for triangle in triangles:
                if triangle == best_one:
                    triangles.remove(triangle)
            if not passed:
                continue
            passed = False
            for max_amount in MAX_ORDER_AMOUNTS:
                if best_one[0][0]['main_coin'] in max_amount['coins']:
                    max_amount['max_amount'] -= best_one[0][0]['amount'] * convert_orderbooks[best_one[0][0]['pair'].split('/')[0]]

            # if best_one[0][0]['side'] == 'buy':
            #     best_one[0][0]['price'] += best_one[0][0]['ticksize']
            # else:
            #     best_one[0][0]['price'] -= best_one[0][0]['ticksize']
            # print(MAX_ORDER_AMOUNTS)
            orders.append(best_one[0])
    return orders

def real_profit_counter(filled_amount, order_chain):
    actual_takers = {}
    for taker in range(1, 3):
        taker_rates = fetch_orderbook(order_chain[taker]['pair'], 20)
        bids_asks = 'bids' if order_chain[taker]['side'] == 'sell' else 'asks'
        if taker == 1:
            order_chain[taker - 1]['amount'] = filled_amount
        previous_chain = order_chain[taker - 1]
        if order_chain[taker]['side'] == 'sell' and previous_chain['side'] == 'sell':
            amount = previous_chain['amount'] * previous_chain['price']
        if order_chain[taker]['side'] == 'sell' and previous_chain['side'] == 'buy':
            amount = previous_chain['amount']
        if order_chain[taker]['side'] == 'buy' and previous_chain['side'] == 'sell':
            amount = previous_chain['amount'] * previous_chain['price'] / order_chain[taker]['price']
        if order_chain[taker]['side'] == 'buy' and previous_chain['side'] == 'buy':
            amount = previous_chain['amount'] / order_chain[taker]['price']
        order_chain[taker]['amount'] = amount
        price = order_chain[taker]['price']
        depth = order_chain[taker]['depth']
        for order in taker_rates[bids_asks]:
            amount -= order[1]
            if amount <= 0:
                depth = taker_rates[bids_asks].index(order)
                price = order[0]
                break
        if bids_asks == 'asks':
            price_diff = (order_chain[taker]['price'] - price) / order_chain[taker]['price'] * 100
        else:
            price_diff = (price - order_chain[taker]['price']) / order_chain[taker]['price'] * 100
        # if price_diff < -0.5:
            # print(f"Filled amount {filled_amount}")
            # print(f"Orderbook {taker_rates}")
            # print(f"Order chain {order_chain}")
        actual_takers.update({'taker_' + str(taker): {'act_depth': depth, 'act_price': price, 'price_diff': price_diff, 'triangle_price': order_chain[taker]['price']}})
        order_chain[taker]['price'] = price
    return actual_takers


def deal_profit_count(filled_amount, order_chain):
    filled_kf = filled_amount / order_chain[0]['amount']
    unchecked_comission = 0.003 * (1 - filled_kf)
    comission = (0.009 - unchecked_comission) * order_chain[0]['convert_orderbooks']['WAVES']
    temp_am = order_chain[0]['amount']
    order_chain[0]['amount'] = filled_amount
    actual_takers = real_profit_counter(filled_amount, order_chain)
    # for taker in range(1, 3):
    #     order_chain[taker]['price'] = actual_takers['taker_' + str(taker)]['act_price']
    #     order_chain[taker]['depth'] = actual_takers['taker_' + str(taker)]['depth']
    if order_chain[0]['side'] == 'sell':
        coin = order_chain[0]['pair'].split('/')[0]
        if coin != 'USDN':
            initial_amount = filled_amount + comission / order_chain[0]['convert_orderbooks'][coin]
        else:
            initial_amount = filled_amount + comission
    else:
        coin = order_chain[0]['pair'].split('/')[1]
        if coin != 'USDN':
            initial_amount = filled_amount * order_chain[0]['price'] + comission / order_chain[0]['convert_orderbooks'][coin]
        else:
            initial_amount = filled_amount * order_chain[0]['price'] + comission
    # КОНВЕРТАЦИИ
    for chain in order_chain[1:]:
        previous_chain = order_chain[order_chain.index(chain) - 1]
        if chain['side'] == 'sell' and previous_chain['side'] == 'sell':
            chain['amount'] = previous_chain['amount'] * previous_chain['price']
        if chain['side'] == 'sell' and previous_chain['side'] == 'buy':
            chain['amount'] = previous_chain['amount']
        if chain['side'] == 'buy' and previous_chain['side'] == 'sell':
            chain['amount'] = previous_chain['amount'] * previous_chain['price'] / chain['price']
        if chain['side'] == 'buy' and previous_chain['side'] == 'buy':
            chain['amount'] = previous_chain['amount'] / chain['price']
    if order_chain[2]['side'] == 'sell':
        end_amount = order_chain[2]['amount'] * order_chain[2]['price']
    else:
        end_amount = order_chain[2]['amount']
    profit = round((end_amount - initial_amount) / initial_amount * 100, 12)
    profit_abs = round(end_amount - initial_amount, 12)
    order_chain[0]['amount'] = temp_am
    return order_chain, coin, profit, profit_abs, actual_takers


def converts_assets(order_chain):
    if order_chain['side'] == 'sell':
        start_coin = order_chain['pair'].split('/')[0]
        deal_coin = order_chain['pair'].split('/')[1]
        try:
            amount_convert = order_chain['price']
        except:
            amount_convert = order_chain['price']
        initial_amount = order_chain['amount']
    else:
        start_coin = order_chain['pair'].split('/')[1]
        deal_coin = order_chain['pair'].split('/')[0]
        try:
            amount_convert = 1 / order_chain['price']
            initial_amount = round(order_chain['amount'] * order_chain['price'], 8)
        except:
            amount_convert = 1 / order_chain['price']
            initial_amount = round(order_chain['amount'] * order_chain['price'], 8)
    return start_coin, deal_coin, amount_convert, initial_amount

def return_string_price(price):
    try:
        if 'e' in str(price):
            price = str(price)
            parts = re.search(r'\.(\d+)e.(\d+)', price)
            res = len(parts.group(1))
            exp = int(parts.group(2)) + res
            return "{:.{exp}f}".format(float(price), exp=exp)
        else:
            return price
    except:
        return price


def partial_message(actual_takers, order_chain, last_order, profit, profit_abs, coin, proc_num):
    message = f"Process num: {proc_num}\n"
    message += f"Maker: {order_chain[0]['pair']} ({order_chain[0]['side']})\n"
    message += f"Initial order amount {order_chain[0]['amount']}\n"
    message += f"Price {order_chain[0]['price']}\n"
    message += f"Executed for {return_string_price(last_order['filled'])} {order_chain[0]['pair'].split('/')[0]} ({round(last_order['filled'] / order_chain[0]['amount'] * 100, 2)} %)\n"
    message += f"Expected profit {return_string_price(profit_abs)} {coin} ({profit} %)\n"
    message += f"Taker 1: {actual_takers['taker_1']['triangle_price']} -> {actual_takers['taker_1']['act_price']} ({round(actual_takers['taker_1']['price_diff'], 4)}), {actual_takers['taker_1']['act_depth'] + 1}\n"
    message += f"Taker 2: {actual_takers['taker_2']['triangle_price']} -> {actual_takers['taker_2']['act_price']} ({round(actual_takers['taker_2']['price_diff'], 4)}), {actual_takers['taker_2']['act_depth'] + 1}\n"
    message += f"Taker orders wasnt placed"
    return message

def tg_order_making(taker_1_id, taker_2_id, replace_data, actual_takers, order_count_time, order_execute_time, start_date, order_chain, last_order, profit, profit_abs, order_time, cancel_order_time, order_check_time, deal_count, total_profit):
    to_base = {'order_place_date': order_time, 
    'order_execute_date': str(datetime.datetime.now(datetime.timezone(offset))).split(' ')[1].split('.')[0],
    'triangle': f"{order_chain[0]['pair']} -> {order_chain[1]['pair']} -> {order_chain[2]['pair']}", 
    'maker_pair': order_chain[0]['pair'],
    'maker_side': order_chain[0]['side'],
    'maker_order_price': order_chain[0]['price'],
    'maker_order_position': order_chain[0]['position'],
    'taker_pair_1': order_chain[1]['pair'],
    'taker_1_side': order_chain[1]['side'],
    'taker_1_order_price': order_chain[1]['price'],
    'taker_1_depth': order_chain[1]['depth'] + 1,
    'taker_pair_2': order_chain[2]['pair'],
    'taker_2_side': order_chain[2]['side'],
    'taker_2_order_price': order_chain[2]['price'],
    'taker_2_depth': order_chain[2]['depth'] + 1,
    'deal_result_perc': profit,
    'deal_result_abs': return_string_price(profit_abs),
    'taker_1_id': taker_1_id,
    'taker_2_id': taker_2_id
    }
    message_1 = f"Order #{deal_count} / {order_execute_time.split('.')[0]}\n"
    message_1 += f"Bot session lifetime {datetime.datetime.now() - start_date}\n"
    message_1 += f"Process num: {replace_data['proc_num']}\n\n"
    message_2 = f"Maker: {order_chain[0]['pair']} ({order_chain[0]['side']})\n"
    message_2 += f"Initial order amount: {round(order_chain[0]['amount'], 6)} {order_chain[0]['pair'].split('/')[0]}\n"
    message_2 += f"Pair spread {round(order_chain[0]['spread'], 4)} %\n"
    message_2 += f"Position in spread {round(order_chain[0]['position'], 4)} %\n"
    filled_kf = round(last_order['filled'] / order_chain[0]['amount'] * 100, 2)
    message_2 += f"Executed: {round(last_order['filled'], 6)} {order_chain[0]['pair'].split('/')[0]} ({filled_kf} %)\n"
    order_chain[0]['amount'] = last_order['filled']
    start_coin, asset_coin, amount_convert, initial_amount = converts_assets(order_chain[0])
    to_base.update({'maker_coin_amount': initial_amount, 
                'execute_percent': filled_kf,
                'maker_deal_result': round(initial_amount * amount_convert, 8),
                'maker_result': f"{initial_amount} {start_coin} -> {round(initial_amount * amount_convert, 8)} {asset_coin}",
                })
    message_2 += f"price: {round(order_chain[0]['price'], 6)}\n"
    message_2 += f"result: {initial_amount} {start_coin} -> {round(initial_amount * amount_convert, 6)} {asset_coin}\n\n"
    message_3 = f"Taker 1: {order_chain[1]['pair']} ({order_chain[1]['side']}, depth: {order_chain[1]['depth'] + 1})\n"
    start_coin, asset_coin, amount_convert, initial_amount = converts_assets(order_chain[1])
    to_base.update({'taker_1_coin_amount': initial_amount,
                'taker_1_deal_result': round(initial_amount * amount_convert, 8),
                'taker_1_result': f"{round(initial_amount, 8)} {start_coin} -> {round(initial_amount * amount_convert, 8)} {asset_coin}"})
    message_3 += f"price: {round(order_chain[1]['price'], 6)}\n"
    message_3 += f"result: {round(initial_amount, 6)} {start_coin} -> {round(initial_amount * amount_convert, 6)} {asset_coin}\n\n"
    message_4 = f"Taker 2: {order_chain[2]['pair']} ({order_chain[2]['side']}, depth: {order_chain[2]['depth'] + 1})\n"
    start_coin, asset_coin, amount_convert, initial_amount = converts_assets(order_chain[2])
    to_base.update({'taker_2_coin_amount': initial_amount,
                'taker_2_deal_result': round(initial_amount * amount_convert, 8),
                'taker_2_result': f"{round(initial_amount, 8)} {start_coin} -> {round(initial_amount * amount_convert, 8)} {asset_coin}"})
    message_4 += f"price: {round(order_chain[2]['price'], 6)}\n"
    message_4 += f"result: {round(initial_amount, 6)} {start_coin} -> {round(initial_amount * amount_convert, 6)} {asset_coin}\n\n"
    if asset_coin != 'USDN':
        change = fetch_orderbook(asset_coin + '/USDN', 1)
        message_5 = f"Deal profit: {return_string_price(profit_abs)} {asset_coin} ({profit_abs * change['asks'][0][0]} USDN {profit} %)\n"
        total_profit += profit_abs * change['asks'][0][0]
        message_5 += f"Session profit {round(total_profit, 2)} USDN\n\n"
        to_base.update({'profit_coin': asset_coin,
                'profit_USDN': profit_abs * change['asks'][0][0]})
    else:
        message_5 = f"Deal profit: {return_string_price(profit_abs)} {asset_coin} ({profit} %)\n"
        total_profit += profit_abs
        message_5 += f"Session profit {round(total_profit, 2)} USDN\n\n"
        to_base.update({'profit_coin': asset_coin,
                'profit_USDN': profit_abs})
    # taker_1_status = check_order_status(order_chain[1]['pair'], round(order_chain[1]['price'], 6), order_chain[1]['side'])
    # taker_2_status = check_order_status(order_chain[2]['pair'], round(order_chain[2]['price'], 6), order_chain[2]['side'])
    # to_base.update({'taker_1_status': taker_1_status, 'taker_2_status': taker_2_status})
    # if taker_1_status == 'closed':
    #     to_base.update({'taker_1_time': str(datetime.datetime.now(datetime.timezone(offset))).split('.')[0]})
    # else:
    #     to_base.update({'taker_1_time': None})
    # if taker_2_status == 'closed':
    #     to_base.update({'taker_2_time': str(datetime.datetime.now(datetime.timezone(offset))).split('.')[0]})
    # else:
    #     to_base.update({'taker_2_time': None})
    hanging_time = float(order_execute_time.split(':')[2].split('+')[0]) - float(order_time.split(':')[2].split('+')[0]) if float(order_execute_time.split(':')[2].split('+')[0]) - float(order_time.split(':')[2].split('+')[0]) > 0 else 60 + float(order_execute_time.split(':')[2].split('+')[0]) - float(order_time.split(':')[2].split('+')[0])
    message_5 += f"Taker 1: {actual_takers['taker_1']['triangle_price']} -> {actual_takers['taker_1']['act_price']} ({round(actual_takers['taker_1']['price_diff'], 4)}), {actual_takers['taker_1']['act_depth'] + 1}\n"
    message_5 += f"Taker 2: {actual_takers['taker_2']['triangle_price']} -> {actual_takers['taker_2']['act_price']} ({round(actual_takers['taker_2']['price_diff'], 4)}), {actual_takers['taker_2']['act_depth'] + 1}\n"
    message_5 += f"Order hang time {round(hanging_time, 4)}\n"
    message_5 += f"Order place time: {order_time.split('.')[0]}\n"
    try:
        message_5 += f"Order cancelling time: {round(cancel_order_time, 4)}\n"
    except:
        message_5 += f"Order cancelling time: {cancel_order_time}\n"
    message_5 += f"Order check time: {round(order_check_time, 4)}\n"
    message_5 += f"Order profit count time {round(order_count_time, 4)}"
    to_base.update({'order_hang_time': round(hanging_time, 4)})
    base_update(to_base)
    return message_1, message_2, message_3, message_4, message_5, total_profit

def partial_base_generate(order, profit, profit_abs, last_order, order_execute_time, proc_num):
    maker_start_coin, maker_asset_coin, maker_amount_convert, maker_initial_amount = converts_assets(order['order_chain'][0])
    taker_1_start_coin, taker_1_asset_coin, taker_1_amount_convert, taker_1_initial_amount = converts_assets(order['order_chain'][1])
    taker_2_start_coin, taker_2_asset_coin, taker_2_amount_convert, taker_2_initial_amount = converts_assets(order['order_chain'][2])
    hanging_time = float(order_execute_time.split(':')[2].split('+')[0]) - float(order['place_time'].split(':')[2].split('+')[0]) if float(order_execute_time.split(':')[2].split('+')[0]) - float(order['place_time'].split(':')[2].split('+')[0]) > 0 else 60 + float(order_execute_time.split(':')[2].split('+')[0]) - float(order['place_time'].split(':')[2].split('+')[0])
    # if proc_num == 1:
    #     print('Line 1421')
    if taker_2_asset_coin != 'USDN':
        profit_USDN = profit_abs * fetch_orderbook(taker_2_asset_coin + '/USDN', 1)['bids'][0][0]
    else:
        profit_USDN = profit_abs
    # if proc_num == 1:
    #     print('Line 1424')
    to_base = {'order_place_date': order['place_time'],
                'order_execute_date': order_execute_time,
                'triangle': f"{order['order_chain'][0]['pair']} -> {order['order_chain'][1]['pair']} -> {order['order_chain'][2]['pair']}",
                'maker_pair': order['order_chain'][0]['pair'],
                'maker_side': order['order_chain'][0]['side'],
                'execute_percent': round(last_order['filled'] / order['order_chain'][0]['amount'] * 100, 2),
                'maker_order_price': order['order_chain'][0]['price'],
                'maker_order_position': order['order_chain'][0]['position'],
                'maker_coin_amount': maker_initial_amount,
                'maker_deal_result': round(maker_initial_amount * maker_amount_convert, 8),
                'maker_result': f"{maker_initial_amount} {maker_start_coin} -> {round(maker_initial_amount * maker_amount_convert, 8)} {maker_asset_coin}",
                'taker_pair_1': order['order_chain'][1]['pair'],
                'taker_1_side': order['order_chain'][1]['side'],
                'taker_1_order_price': order['order_chain'][1]['price'],
                'taker_1_depth': order['order_chain'][1]['depth'],
                'taker_1_coin_amount': taker_1_initial_amount,
                'taker_1_deal_result': round(taker_1_initial_amount * taker_1_amount_convert, 8),
                'taker_1_result': f"{taker_1_initial_amount} {taker_1_start_coin} -> {round(taker_1_initial_amount * taker_1_amount_convert, 8)} {taker_1_asset_coin}",
                'taker_pair_2': order['order_chain'][2]['pair'],
                'taker_2_side': order['order_chain'][2]['side'],
                'taker_2_order_price': order['order_chain'][2]['price'],
                'taker_2_depth': order['order_chain'][2]['depth'],
                'taker_2_coin_amount': taker_2_initial_amount,
                'taker_2_deal_result': round(taker_2_initial_amount * taker_2_amount_convert, 8),
                'taker_2_result': f"{taker_2_initial_amount} {taker_2_start_coin} -> {round(taker_2_initial_amount * taker_2_amount_convert, 8)} {taker_2_asset_coin}",
                'deal_result_perc': profit,
                'deal_result_abs': profit_abs,
                'profit_coin': taker_2_asset_coin,
                'profit_USDN': profit_USDN,
                'order_hang_time': hanging_time}
    return to_base


def execute_2t(order, replace_data):
    order_check_time_start = time.time()
    last_order = None
    # if not order['order_id'] or not order['order_chain'][0]:
    #     # print(order)
    #     order['order_chain'] = [None, None, None]
    #     replace_data['child_connection'].send('Pause')
    #     return order, replace_data
    # if open_orders:
    #     find_order = [x for x in open_orders if x['id'] == order['order_id']]
    #     if find_order:
    #         last_order = find_order[0]
    #     else:
    #         last_order = fetch_order(order['order_chain'][0]['pair'], order['order_id'])
    # if not last_order:
    # if replace_data['proc_num'] == 1:
    #     print('line 1469')
    last_order = fetch_order(order['order_chain'][0]['pair'], order['order_id'])
    order_check_time = time.time() - order_check_time_start
    cancel_order_time = 'order fully bought'
    # if not last_order:
    #     order['order_id'] = None
    #     order['order_chain'] = [None, None, None]
    #     replace_data['child_connection'].send('Pause')
    #     return order, replace_data
    # if replace_data['proc_num'] == 1:
    #     print('line 1479')
    if not last_order:
        return order, replace_data
    if last_order['filled'] > 0:
        order_execute_time = str(datetime.datetime.now(datetime.timezone(offset))).split(' ')[1]
        order_count_time = time.time()
        last_amount = order['order_chain'][0]['amount']
        # if replace_data['proc_num'] == 1:
        #     print('line 1485')
        order['order_chain'], coin, profit, profit_abs, actual_takers = deal_profit_count(last_order['filled'], order['order_chain'])
        order_count_time = time.time() - order_count_time
        # if replace_data['proc_num'] == 1:
        #     print('line 1489')
        if profit_abs < 0:
            # if last_order['filled'] / last_amount > 0.5:
                # print(f"Uncounted {order}")
            # if replace_data['proc_num'] == 1:
            #     print('line 1494')
            message = partial_message(actual_takers, order['order_chain'], last_order, profit, profit_abs, coin, replace_data['proc_num'])
            # if replace_data['proc_num'] == 1:
            #     print('line 1497')
            part_filter = (order['order_chain'][0]['pair'], last_order['filled'])
            if part_filter not in replace_data['parts']:
                # if replace_data['proc_num'] == 1:
                #     print('line 1501')
                to_base_partial = partial_base_generate(order, profit, profit_abs, last_order, order_execute_time, replace_data['proc_num'])
                # if replace_data['proc_num'] == 1:
                #     print('line 1504')
                base_partial_update(to_base_partial)
                # if replace_data['proc_num'] == 1:
                #     print('line 1507')
                cancel_order(order['order_id'])
                # if replace_data['proc_num'] == 1:
                #     print('line 1510')
                order['order_chain'] = [None, None, None]
                order['order_id'] = None
                replace_data['partial_deal_count'] += 1
                replace_data['parts'].append(part_filter)
                message = f"Partial order #{replace_data['partial_deal_count']}\n\n" + message
                message += f"\nPlace time {order['place_time'].split('.')[0]}"
                message += f"\nExecute time {str(datetime.datetime.now(datetime.timezone(offset))).split(' ')[1].split('.')[0]}"
                try:
                    telegram_bot.send_message(chat_id, message) 
                    # if replace_data['proc_num'] == 1:
                    #     print('line 1521')  
                except:
                    pass
                    # telegram_bot.send_message(chat_id, message)
                # if replace_data['proc_num'] == 1:
                #     print('line 1526')
                check_balance(replace_data['start_amounts'][0], project_start_balance = replace_data['start_amounts'][1])
        else:
            # if replace_data['proc_num'] == 1:
            #     print('line 1530')
            replace_data['deal_count'] += 1
            replace_data['parent_connection_2'].send(order['order_chain'][1])
            # if replace_data['proc_num'] == 1:
            #     print('line 1534')
            blocked_coin = order['order_chain'][2]['pair'].split('/')[0] if order['order_chain'][2]['side'] == 'sell' else order['order_chain'][2]['pair'].split('/')[1]
            start_time = time.time()
            open_orders = fetch_open_orders()
            if open_orders:
                for deal in open_orders:
                    if deal['symbol'].split('/')[0] == blocked_coin and deal['side'] == 'sell':
                        cancel_order(deal['id'])
                    if deal['symbol'].split('/')[1] == blocked_coin and deal['side'] == 'buy':
                        cancel_order(deal['id'])
                time.sleep(0.2)
            # for block_order in replace_data['orders']:
            #     if block_order['order_chain'][0]:
            #         if block_order['order_chain'][0]['side'] == 'sell' and block_order['order_chain'][0]['pair'].split('/')[0] == blocked_coin:
            #             check_execute(open_orders, block_order, replace_data)
            #             block_order['order_chain'] = [None, None, None]
            #             block_order['order_id'] = None
            #         elif block_order['order_chain'][0]['side'] == 'buy' and block_order['order_chain'][0]['pair'].split('/')[1] == blocked_coin:
            #             check_execute(open_orders, block_order, replace_data)
            #             block_order['order_chain'] = [None, None, None]
            #             block_order['order_id'] = None
            # cancel_time = time.time() - start_time
            replace_data['parent_connection_3'].send(order['order_chain'][2])
            # if replace_data['proc_num'] == 1:
            #     print('line 1550')
            count = 0
            while not replace_data['parent_connection_2'].poll():
                if count > 2000:
                    break
                count += 1 
                time.sleep(0.001)
            # if replace_data['proc_num'] == 1:
            #     print('line 1554')
            try:
                taker_1_id = replace_data['parent_connection_2'].recv()
            except:
                taker_1_id = None
            # if replace_data['proc_num'] == 1:
            #     print('line 1557')
            count = 0
            while not replace_data['parent_connection_3'].poll():
                if count > 2000:
                    break
                count += 1 
                time.sleep(0.001)
            # if replace_data['proc_num'] == 1:
            #     print('line 1561')
            try:
                taker_2_id = replace_data['parent_connection_3'].recv()
            except:
                taker_2_id = None
            # if replace_data['proc_num'] == 1:
            #     print('line 1564')
            # if last_order['status'] != 'closed':
            cancel_order_start_time = time.time()
            cancelling_order = cancel_order(order['order_id'])
            # if replace_data['proc_num'] == 1:
            #     print('line 1569')
            cancel_order_time = time.time() - cancel_order_start_time
            message_1, message_2, message_3, message_4, message_5, replace_data['total_profit'] = tg_order_making(taker_1_id, taker_2_id, replace_data, actual_takers, order_count_time, order_execute_time, replace_data['start_date'], order['order_chain'], last_order, profit, profit_abs, order['place_time'], cancel_order_time, order_check_time, replace_data['deal_count'], replace_data['total_profit'])
            try:
                telegram_bot.send_message(chat_id, f'✅{message_1}🔴{message_2}🟢{message_3}🟢{message_4}💰{message_5}', parse_mode="Markdown")
            except:
                pass
                # telegram_bot.send_message(chat_id, f'✅{message_1}🔴{message_2}🟢{message_3}🟢{message_4}💰{message_5}', parse_mode="Markdown") # + f'\nCancel block order time {round(cancel_time, 4)}'
            check_balance(replace_data['start_amounts'][0], project_start_balance = replace_data['start_amounts'][1])
            order['order_chain'] = [None, None, None]
            order['order_id'] = None
    return order, replace_data


def check_execute(order, replace_data):
    # last_order = fetch_order(order['order_chain'][0]['pair'], order['order_id'])
    # last_order = find_order[0] if len(find_order) else fetch_order(order['order_chain'][0]['pair'], order['order_id'])
    # find_order = [x for x in open_orders if x['id'] == order['order_id']]
    # find_order = fetch_order(order['order_chain'][0]['pair'], order['order_id'])
    cancelling_order = cancel_order(order['order_id'])
    order, replace_data = execute_2t(order, replace_data)
    return order, replace_data

def replacing_order_m2t(child_connection, parent_connection_2, parent_connection_3, proc_num):
    orders_num = 3
    replace_data = {'orders': [{'order_chain': [None, None, None], 'order_id': None, 'place_time': None} for x in range(orders_num)],
                    'start_date': datetime.datetime.now(),
                    'deal_count': 0,
                    'partial_deal_count': 0,
                    'parts': [],
                    'start_amounts': check_balance(),
                    'child_connection': child_connection,
                    'parent_connection_2': parent_connection_2,
                    'parent_connection_3': parent_connection_3,
                    'total_profit': 0,
                    'proc_num': proc_num}
    child_connection.send(replace_data['start_amounts'])
    count = 0
    if proc_num == 1:
        indexes = [0, 3, 6]
    elif proc_num == 2:
        indexes = [1, 4, 7]
    else:
        indexes = [2, 5, 8]
    while True:
        try:
            # start_time = time.time()
            # if proc_num == 1:
            #     print('Line 1574')
            # print(f"Replacer {proc_num}")
            time.sleep(0.001)
            # if proc_num == 1:
            #     print('Line 1577')
            for order in replace_data['orders']:
                if order['order_chain'][0]:
                    # if proc_num == 1:
                    #     print('line 1580')
                    order, replace_data = execute_2t(order, replace_data)
            # print(f"Check orders time: {time.time() - start_time} sec")
            count += 1
            if count > 5:
                # if proc_num == 1:
                #     print('Line 1584')
                # print(f"Replacer {proc_num} Start replace")
                count = 0
                for order_cancel in replace_data['orders']:
                    if not order_cancel['order_chain'][0]:
                        continue
                    else:
                        # if proc_num == 1:
                        #     print(f"Replacer {proc_num} Cancelling order")
                        start_deal_count = replace_data['deal_count']
                        start_partial_deal_count = replace_data['partial_deal_count']
                        order_cancel, replace_data = check_execute(order_cancel, replace_data)
                        order_cancel['order_chain'] = [None, None, None]
                        order_cancel['order_id'] = None           
                time.sleep(.2)
                # print(f"Replacer {proc_num} Start fetching new orders")
                # start = time.time()
                with open('data_file.json', 'r') as file:
                    try:
                        loaded = json.load(file)
                    except:
                        continue
                # print(f"Reading json file time {time.time() - start} sec")
                new_orders = [x for x in loaded if loaded.index(x) in indexes]
                for b in loaded:
                    if loaded.index(b) in indexes:
                        loaded.pop(loaded.index(b))
                with open('data_file.json', 'w') as file:
                    try:
                        json.dump(loaded, file)
                    except:
                        pass
                # print(f"Replacer {proc_num} Start placing new orders")
                for order in new_orders:
                    if order:
                        # if proc_num == 1:
                        #     print(f"Replacer {proc_num} placing new order")
                        order_result = place_order(order[0])
                        # if proc_num == 1:
                        #     print(f"Replacer {proc_num} order placed")
                        if not order_result['success']:
                            # if proc_num == 1:
                            #     print(f"Replacer {proc_num} order error\n{order_result}")
                            for order_cancel in replace_data['orders']:
                                # if proc_num == 1:
                                #     print('Order cancelling cycle line 1624')
                                if not order_cancel['order_chain'][0]:
                                    continue
                                else:
                                    # if proc_num == 1:
                                    #     print(f"Replacer {proc_num} check-executing order start")
                                    start_deal_count = replace_data['deal_count']
                                    start_partial_deal_count = replace_data['partial_deal_count']
                                    order_cancel, replace_data = check_execute(order_cancel, replace_data)
                                    # if proc_num == 1:
                                    #     print('line 1633')
                                    # if proc_num == 1:
                                    #     print(f"Replacer {proc_num} check-execute completed")
                                    order_cancel['order_chain'] = [None, None, None]
                                    order_cancel['order_id'] = None
                            time.sleep(.2)
                            continue
                        replace_data['orders'][new_orders.index(order)]['order_chain'] = order
                        replace_data['orders'][new_orders.index(order)]['place_time'] = str(datetime.datetime.now(datetime.timezone(offset))).split(' ')[1]
                        replace_data['orders'][new_orders.index(order)]['order_id'] = order_result['message']['id']
        except Exception as e:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            try:
                telegram_bot.send_message(chat_id, f"Replacer error {e}. Error on line {exc_tb.tb_lineno}")
            except:
                pass
            # print(exc_type, fname, exc_tb.tb_lineno)

orders_num = 9
with open("data_file.json", "w") as file:
    json.dump([False for x in range(orders_num)], file)


p_conn_1, c_conn_1 = Pipe()
p_conn_2, c_conn_2 = Pipe()
p_conn_3, c_conn_3 = Pipe()
p_conn_4, c_conn_4 = Pipe()
p_conn_5, c_conn_5 = Pipe()
if __name__ == '__main__':
    procs = []
    # proc = Process(target=triangle_trading_waves, args = (parent_connection_1, data, ))
    for connection in enumerate([c_conn_1, c_conn_4, c_conn_5]):
        proc = Process(target=replacing_order_m2t, args = (connection[1], p_conn_2, p_conn_3, connection[0] + 1, ))
        procs.append(proc)
        proc.start()
    for order in enumerate([c_conn_2, c_conn_3]):
        proc = Process(target=open_order, args=(order[0], order[1], ))
        procs.append(proc)
        proc.start()
# while True:
# try:
triangle_trading_waves(p_conn_1, p_conn_4, p_conn_5, data)
# # replacing_order_m2t(child_connection_1, parent_connection_2, parent_connection_3)
# except Exception as e:
#     try:
#         telegram_bot.send_message(chat_id, f'Parser crushed.\n{e}')
#     except:
#         pass
            # telegram_bot.send_message(chat_id, f'Parser crushed.\n{e}' + ' EXCEPT')
        # with open('bot_log.txt', 'a', encoding='UTF-8') as log:
        #     log.write(str(e) + '\n')
