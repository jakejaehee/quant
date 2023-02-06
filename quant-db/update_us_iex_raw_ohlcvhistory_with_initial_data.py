import datetime
import time

import requests
from tqdm import tqdm

import db_util

import asyncio
semaphore = asyncio.Semaphore(100)

import asyncpg


class DB:
    def __init__(self):
        self.conn = db_util._conn()
        self.config = db_util._config()
        self.iex_cloud_api_token = db_util.get_iex_token(self.config)

    def update_us_iex_raw_ohlcvhistory_table_with_initial_data(self, time_range):
        all_symbol_list = get_symbol_list_to_update_from_symbol_meta(self.conn)
        try:
            symbol_list_already_updated = get_symbol_list_already_updated(self.conn)
            symbol_set_to_update = set(i[0] for i in all_symbol_list) - set(i[0] for i in symbol_list_already_updated)
            symbol_list = [k for k in all_symbol_list if k[0] in symbol_set_to_update]
        except:
            pass
        for symbol, exchange in tqdm(symbol_list):
            try:
                ohlcvhistory = get_symbol_ohlcvhistory(symbol, time_range, self.iex_cloud_api_token)
                asyncio.run(insert_symbol_ohlcvhistory(symbol, exchange, ohlcvhistory))
                print(f'{symbol} on {exchange} DONE')
                time.sleep(0.1)
            except:
                print(f'{symbol} on {exchange} FAILED')


def get_symbol_list_to_update_from_symbol_meta(conn):
    cur = conn.cursor()
    query = '''
    SELECT symbol, exchange
    FROM US_IEX_SYMBOL_META
    '''
    cur.execute(query)
    result = cur.fetchall()
    return result


def get_symbol_list_already_updated(conn):
    cur = conn.cursor()
    query = '''
        select symbol, max(price_date)
        from us_iex_raw_ohlcvhistory
        group by symbol
        having max(price_date) = current_date - integer '1'
    '''
    cur.execute(query)
    result = cur.fetchall()
    return result


def get_symbol_ohlcvhistory(symbol, time_range, iex_cloud_api_token):
    response = requests.get(
        f"https://cloud.iexapis.com/stable/stock/{symbol}/chart/{time_range}",
        params={
            "token": iex_cloud_api_token
        }
    )
    symbol_ohlcvhistory = response.json()
    return symbol_ohlcvhistory


async def insert_symbol_ohlcvhistory(symbol, exchange, ohlcvhistory):
    config = db_util._config()
    host = config['DB_CONNECTION_INFO']['host']
    dbname = config['DB_CONNECTION_INFO']["dbname"]
    user = config['DB_CONNECTION_INFO']["user"]
    password = config['DB_CONNECTION_INFO']["password"]
    port = config['DB_CONNECTION_INFO']["port"]

    values = [(exchange, datetime.datetime.strptime(row['date'], '%Y-%m-%d'), symbol,
               row.get('open'), row.get('high'), row.get('low'), row.get('close'), row.get('volume'),
               row.get('uOpen'), row.get('uHigh'), row.get('uLow'), row.get('uClose'), row.get('uVolume'),
               row.get('fOpen'), row.get('fHigh'), row.get('fLow'), row.get('fClose'), row.get('fVolume')) for row in ohlcvhistory]

    conn = await asyncpg.connect(user=user, password=password,
                                 database=dbname, host=host, port=port)

    query = '''
    INSERT INTO US_IEX_RAW_OHLCVHISTORY
    (exchange, price_date, symbol, open, high, low, close, volume,
    u_open, u_high, u_low, u_close, u_volume,
    f_open, f_high, f_low, f_close, f_volume)
    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18)
    ON CONFLICT (exchange, price_date, symbol) DO NOTHING;
    '''
    await conn.executemany(query, values)


if __name__ == '__main__':
    db = DB()
    db.update_us_iex_raw_ohlcvhistory_table_with_initial_data('max')
    #db.update_us_iex_raw_ohlcvhistory_table_with_initial_data('5d')
