import sqlite3
import datetime

class triangle_database:

    def __init__(self, telegram_bot, chat_id):
        self.chat_id = chat_id
        self.telegram_bot = telegram_bot

        self.connect = sqlite3.connect('deals.db')
        self.sql_create_orders_table()
        self.sql_create_partial_orders_table()
        self.sql_create_balances_table()

    def sql_create_balances_table(self):
        cursor = self.connect.cursor()
        cursor.execute("""CREATE TABLE IF NOT EXISTS balances (
        record_num INTEGER PRIMARY KEY AUTOINCREMENT,
        datetime TIMESTAMP,
        session_id TEXT,
        coins TEXT,
        balances TEXT,
        usdBalances TEXT,
        totalUsdValue REAL
        );""")
        self.connect.commit()
        cursor.close()

    def sql_balances_update(self, balances, changes, session_id):
        cursor = self.connect.cursor()
        coins = ''
        amounts = ''
        usd_amounts = ''
        total_usd_amount = 0
        for balance in balances.values():
            if balance.total_balance != '0':
                coins += f'{balance.currency}/'
                amounts += f'{balance.total_balance}/'
                usd_amounts += f'{round(float(balance.total_balance) * changes[balance.currency])}/'
                total_usd_amount += float(balance.total_balance) * changes[balance.currency]
        sql = f"""INSERT INTO balances (
        datetime,
        session_id,
        coins,
        balances,
        usdBalances,
        totalUsdValue)
        VALUES ('{datetime.datetime.now()}',
        '{session_id}',
        '{coins}', 
        '{amounts}',
        '{usd_amounts}',
        {round(total_usd_amount)})"""
        cursor.execute(sql)
        self.connect.commit()
        cursor.close()

    def sql_create_orders_table(self):
        cursor = self.connect.cursor()
        cursor.execute("""CREATE TABLE IF NOT EXISTS deals (
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
        profit_USD REAL,
        order_hang_time REAL
        );""")
        self.connect.commit()
        cursor.close()

    def sql_create_partial_orders_table(self):
        cursor = self.connect.cursor()
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
        profit_USD REAL,
        order_hang_time REAL
        );""")
        self.connect.commit()
        cursor.close()

    def base_update(self, to_base):
        cursor = self.connect.cursor()
        sql = f"""INSERT INTO deals (
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
        profit_USD,
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
        {to_base["profit_USD"]},
        {to_base["order_hang_time"]}
        )"""
        try:
            cursor.execute(sql)
        except Exception as e:
            telegram_bot.send_message(chat_id, f"DB error {e}\nData {sql}")
        self.connect.commit()
        cursor.close()

    def base_partial_update(self, to_base):
        cursor = self.connect.cursor()
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
        profit_USD,
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
        {to_base["profit_USD"]},
        {to_base["order_hang_time"]}
        )"""
        try:
            cursor.execute(rf"{sql}")
        except Exception as e:
            self.telegram_bot.send_message(self.chat_id, f"DB partial error {e}\nData {sql}")
        self.connect.commit()
        cursor.close()

    def fetch_data_from_table(self, table):
        if not table in ['partial_deals', 'deals', 'balances']:
            raise Exception('Have only tables: partial_deals, deals, balances')
        cursor = self.connect.cursor()
        data = cursor.execute(f"SELECT * FROM {table};").fetchall()
        cursor.close()
        return data


    def close_connection(self):
        self.connect.close()

