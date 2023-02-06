import psycopg
import os
import configparser

def _config():
    _path = os.path.join("config", 'config.ini')
    if not os.path.exists(_path):
        raise Exception("There is no config file, please run config.py")
    config = configparser.ConfigParser()
    config.read(_path)
    return config

def get_connection_information(config):
    host = config['DB_CONNECTION_INFO']['host']
    dbname = config['DB_CONNECTION_INFO']["dbname"]
    user = config['DB_CONNECTION_INFO']["user"]
    password = config['DB_CONNECTION_INFO']["password"]
    port = config['DB_CONNECTION_INFO']["port"]
    return f"host={host} dbname={dbname} user={user} password={password} port={port}"

def _conn():
    config = _config()
    conn = psycopg.connect(get_connection_information(config))
    return conn

def get_iex_token(config):
    return config['EXTERNAL_CONNECTION_INFO']['iex_cloud_api_token']

if __name__ == '__main__':
    pass