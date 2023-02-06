import datetime

import exchange_calendars as xcals

import asyncio
semaphore = asyncio.Semaphore(100)


class xcal_request:
    def __init__(self):
        print('init')

    def check_exchange_calendars(self):
        exchange_list = get_exchange_list()
        trading_days = get_trading_days('XNYS')
        for exchange in exchange_list:
            print_trading_days(exchange, trading_days)
        print('update_exchange_calendars DONE')


def get_exchange_list():
    exchange_list = ['ARCX', 'BATS', 'XASE', 'XNAS', 'XNYS', 'XPOR']
    return exchange_list

def get_trading_days(exchange):
    start = "2010-01-01"
    xcalendar = xcals.get_calendar(exchange)
    result = xcalendar.sessions_in_range(start, datetime.date.today())
    return result


def print_trading_days(exchange, trading_days):
    for i, price_date in enumerate(trading_days):
        print('''
            INSERT INTO EXCHANGE_CALENDARS (exchange, price_date, di)
            VALUES (%s, %s, %s)
            ON CONFLICT DO NOTHING;
        ''', (exchange, price_date, i + 10000))


if __name__ == '__main__':
    handler = xcal_request()
    handler.check_exchange_calendars()
