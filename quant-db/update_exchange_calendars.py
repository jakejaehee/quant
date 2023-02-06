import xcal_request

import db_util

import asyncio
semaphore = asyncio.Semaphore(100)

class DB:
    def __init__(self):
        self.conn = db_util._conn()
        self.config = db_util._config()

    def update_exchange_calendars_table(self):
        exchange_list = xcal_request.get_exchange_list()
        trading_days = xcal_request.get_trading_days('XNYS')
        for exchange in exchange_list:
            insert_trading_days(self.conn, exchange, trading_days)
        print('update_exchange_calendars DONE')


def insert_trading_days(conn, exchange, trading_days):
    cur = conn.cursor()
    for i, price_date in enumerate(trading_days):
        cur.execute('''
            INSERT INTO EXCHANGE_CALENDARS (exchange, price_date, di)
            VALUES (%s, %s, %s)
            ON CONFLICT DO NOTHING;
        ''', (exchange, price_date, i + 10000))
    conn.commit()
    cur.close()


if __name__ == '__main__':
    db = DB()
    db.update_exchange_calendars_table()
