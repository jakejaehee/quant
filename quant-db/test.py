all_symbol_list = [('AAPL', 'XNAS'), ('TSLA','XNAS'), ('ACIO','BATS')]
print("all_symbol_list=" + all_symbol_list.__str__())

tmp2 = [('TSLA','XNAS')]
# symbol_list = [j for j in set(i[0] for i in symbol_list) - set(i[0] for i in tmp2)]

print(set(i[0] for i in all_symbol_list))
print(set(i[0] for i in tmp2))

symbol_set_to_update = set(i[0] for i in all_symbol_list) - set(i[0] for i in tmp2)
print("symbol_set_to_update=" + symbol_set_to_update.__str__())

result = [k for k in all_symbol_list if k[0] in symbol_set_to_update]
print("result=" + result.__str__())

import datetime

last_date_str = None #'2023-02-04'
last_date = datetime.datetime.strptime(last_date_str, '%Y-%m-%d').date() if last_date_str else datetime.datetime.today().date()
print(last_date_str)
print(last_date)

import db_util

config = db_util._config()
host = config['DB_CONNECTION_INFO']['host']
dbname = config['DB_CONNECTION_INFO']["dbname"]
user = config['DB_CONNECTION_INFO']["user"]
password = config['DB_CONNECTION_INFO']["password"]
port = config['DB_CONNECTION_INFO']["port"]
print(host)
print(password)

