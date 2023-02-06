import db_util
import json
import datetime

import asyncio
semaphore = asyncio.Semaphore(100)

import asyncpg

class DB:
    def __init__(self):
        self.conn = db_util._conn()
        self.config = db_util._config()

    def update_us_iex_ohlcvhistory_with_range(self, first_date_str, last_date_str):
        price_date = datetime.datetime.strptime(first_date_str, '%Y-%m-%d').date()
        last_date = datetime.datetime.strptime(last_date_str, '%Y-%m-%d').date() if last_date_str else datetime.datetime.today().date()

        while price_date <= last_date:
            self.update_us_iex_ohlcvhistory(price_date.strftime("%Y-%m-%d"))
            price_date += datetime.timedelta(days=1)


    def update_us_iex_ohlcvhistory(self, price_date_str):
        raw_ohlcvhistory = get_us_iex_raw_ohlcvhistory_in_pivot(self.conn, price_date_str)
        if len(raw_ohlcvhistory) > 0:
            asyncio.run(upsert_us_iex_ohlcvhistory(raw_ohlcvhistory))


def get_us_iex_raw_ohlcvhistory_in_pivot(conn, price_date_str):
    cur = conn.cursor()
    query = f'''
select exchange, di,
    jsonb_object_agg(ii, open) as open,
    jsonb_object_agg(ii, high) as high,
    jsonb_object_agg(ii, low) as low,
    jsonb_object_agg(ii, close) as close,
    jsonb_object_agg(ii, volume) as volume,
    jsonb_object_agg(ii, u_open) as u_open,
    jsonb_object_agg(ii, u_high) as u_high,
    jsonb_object_agg(ii, u_low) as u_low,
    jsonb_object_agg(ii, u_close) as u_close,
    jsonb_object_agg(ii, u_volume) as u_volume,
    jsonb_object_agg(ii, f_open) as f_open,
    jsonb_object_agg(ii, f_high) as f_high,
    jsonb_object_agg(ii, f_low) as f_low,
    jsonb_object_agg(ii, f_close) as f_close,
    jsonb_object_agg(ii, f_volume) as f_volume
from (
    select r.exchange, r.high, r.open, r.low, r.close, r.volume,
           r.u_open, r.u_high, r.u_low, r.u_close, r.u_volume,
           r.f_open, r.f_high, r.f_low, r.f_close, r.f_volume,
           x.di, m.ii
    from us_iex_raw_ohlcvhistory as r
        left outer join exchange_calendars as x on r.exchange = x.exchange and r.price_date = x.price_date
        left outer join us_iex_symbol_meta as m on r.exchange = m.exchange and r.symbol = m.symbol
    where r.price_date = to_date('{price_date_str}', 'YYYY-MM-DD')
) as a
group by exchange, di
    '''
    cur.execute(query)
    return cur.fetchall()


async def upsert_us_iex_ohlcvhistory(ohlcvhistory):
    query = '''
    insert into us_iex_ohlcvhistory
        (exchange, di, open, high, low, close, volume,
        u_open, u_high, u_low, u_close, u_volume,
        f_open, f_high, f_low, f_close, f_volume)
    values ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17)
    on conflict (exchange, di) do
    update set
	    open = $3, high = $4, low = $5, close = $6, volume = $7,
	    u_open = $8, u_high = $9, u_low = $10, u_close = $11, u_volume = $12,
	    f_open = $13, f_high = $14, f_low = $15, f_close = $16, f_volume = $17
    '''

    values = [(row[0], row[1],
               json.dumps(row[2]), json.dumps(row[3]), json.dumps(row[4]), json.dumps(row[5]), json.dumps(row[6]),
               json.dumps(row[7]), json.dumps(row[8]), json.dumps(row[9]), json.dumps(row[10]), json.dumps(row[11]),
               json.dumps(row[12]), json.dumps(row[13]), json.dumps(row[14]), json.dumps(row[15]), json.dumps(row[16])) for row in ohlcvhistory]

    config = db_util._config()
    host = config['DB_CONNECTION_INFO']['host']
    dbname = config['DB_CONNECTION_INFO']["dbname"]
    user = config['DB_CONNECTION_INFO']["user"]
    password = config['DB_CONNECTION_INFO']["password"]
    port = config['DB_CONNECTION_INFO']["port"]

    conn = await asyncpg.connect(user=user, password=password,
                                 database=dbname, host=host, port=port)
    await conn.executemany(query, values)


if __name__ == '__main__':
    db = DB()
    db.update_us_iex_ohlcvhistory_with_range('2003-02-03', '2010-12-31')
    #db.update_us_iex_ohlcvhistory_with_range('2011-01-01', '2015-12-31')
    #db.update_us_iex_ohlcvhistory_with_range('2016-01-01', '2020-12-31')
    #db.update_us_iex_ohlcvhistory_with_range('2021-01-01', None)

