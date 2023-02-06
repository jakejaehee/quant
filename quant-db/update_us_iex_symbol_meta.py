import datetime

import requests
from tqdm import tqdm

import db_util

import asyncio
semaphore = asyncio.Semaphore(100)


class DB:
    def __init__(self):
        self.conn = db_util._conn()
        self.config = db_util._config()
        self.iex_cloud_api_token = db_util.get_iex_token(self.config)


    def update_us_iex_symbol_meta_table(self):
        symbol_meta = get_symbol_meta(self.iex_cloud_api_token)
        for i, meta in tqdm(enumerate(symbol_meta)):
            if check_db_has_symbol_stat(self.conn, meta):
                continue
            else:
                try:
                    symbol_stats = get_symbol_stats(meta['symbol'], self.iex_cloud_api_token)
                    meta['stat_date'] = datetime.date.today()
                    meta['stat_market_cap'] = symbol_stats['marketcap']
                    meta['stat_shares_outstanding'] = symbol_stats['sharesOutstanding']
                except:
                    print(meta['symbol'])
                    meta['stat_date'] = datetime.date.today()
                    meta['stat_market_cap'] = None
                    meta['stat_shares_outstanding'] = None
            insert_into_table_symbol(self.conn, i, meta)
        print('update_us_iex_symbol_meta_table DONE')


def get_symbol_meta(iex_cloud_api_token):
    response = requests.get(
        "https://cloud.iexapis.com/stable/ref-data/symbols",
        params={
            "token": iex_cloud_api_token
        }
    )
    symbol_meta = response.json()
    return symbol_meta


def get_symbol_stats(symbol, iex_cloud_api_token):
    response = requests.get(
        f"https://cloud.iexapis.com/stable/stock/{symbol}/stats",
        params={
            "token": iex_cloud_api_token
        }
    )
    try:
        symbol_stats = response.json()
    except:
        print(response.text)
    return symbol_stats


def insert_into_table_symbol(conn, i, meta):
    cur = conn.cursor()
    query = '''
    INSERT INTO US_IEX_SYMBOL_META (exchange, symbol, exchange_name, exchange_segment, 
    exchange_segment_name, name, date, type, iex_id, region, currency, is_enabled, figi, cik, lei, ric, status, 
    stat_date, stat_market_cap, stat_shares_outstanding)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT DO NOTHING;
    '''
    cur.execute(query, (meta['exchange'], meta['symbol'], meta['exchangeName'], meta['exchangeSegment'],
                        meta['exchangeSegmentName'], meta['name'], meta['date'], meta['type'], meta['iexId'],
                        meta['region'], meta['currency'], int(meta['isEnabled']), meta['figi'], meta['cik'], meta['lei'],
                        meta.get('ric', None), meta.get('status', 1), meta['stat_date'], meta['stat_market_cap'],
                        meta['stat_shares_outstanding']))
    conn.commit()
    cur.close()


def check_db_has_symbol_stat(conn, meta):
    exchange = meta['exchange']
    symbol = meta['symbol']

    cur = conn.cursor()
    query = "SELECT stat_date, stat_market_cap, stat_shares_outstanding FROM US_IEX_SYMBOL_META where exchange = %s and symbol = %s"
    cur.execute(query, (exchange, symbol))
    result = cur.fetchone()
    if result:
        stat_date, stat_market_cap, stat_shares_outstanding = result
        if stat_date is None or stat_market_cap is None or stat_shares_outstanding is None:
            return False
        elif stat_date == 0 or stat_market_cap == 0 or stat_shares_outstanding == 0:
            return False
        else:
            meta['stat_date'] = stat_date
            meta['stat_market_cap'] = stat_market_cap
            meta['stat_shares_outstanding'] = stat_shares_outstanding
            return True
    else:
        return False


if __name__ == '__main__':
    db = DB()
    db.update_us_iex_symbol_meta_table()
