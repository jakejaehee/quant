import configparser
import json
import os

def make_directory(path):
    if not os.path.exists(path):
        os.mkdir(path)

# noinspection PyBroadException
def DB_connection_setting(config):
    # read DB connection config files
    # noinspection PyBroadException
    try:
        with open(
                os.path.join(
                    config["DEFAULT"]["config_path"], "config_files", "DB_CONNECTION_CONFIG.json"
                ),
                "r",
        ) as f:
            print("DB_CONNECTION_CONFIG.json loaded...")
            connection_config_json = json.load(f)
            config["DB_CONNECTION_INFO"]["host"] = connection_config_json["host"]
            config["DB_CONNECTION_INFO"]["dbname"] = connection_config_json["dbname"]
            config["DB_CONNECTION_INFO"]["user"] = connection_config_json["user"]
            config["DB_CONNECTION_INFO"]["password"] = connection_config_json["password"]
            config["DB_CONNECTION_INFO"]["port"] = connection_config_json["port"]
            print('DB connection setting DONE')

    except:
        while True:
            print("DB connection information file does not appear to exist. Do you want to enter manually? [y/n]:")
            i = input()
            if i == 'y':
                config["DB_CONNECTION_INFO"]["host"] = input("host: ")
                config["DB_CONNECTION_INFO"]["dbname"] = input("dbname: ")
                config["DB_CONNECTION_INFO"]["user"] = input("user: ")
                config["DB_CONNECTION_INFO"]["password"] = input("password: ")
                config["DB_CONNECTION_INFO"]["port"] = input("port: ")
                print('make DB_CONNECTION_INFO.json...')

                with open(os.path.join(
                        config["DEFAULT"]["config_path"], "config_files", "DB_CONNECTION_CONFIG.json"
                ), 'w') as outfile:
                    json.dump({
                        "host": config["DB_CONNECTION_INFO"]["host"],
                        "dbname": config["DB_CONNECTION_INFO"]["dbname"],
                        "user": config["DB_CONNECTION_INFO"]["user"],
                        "password": config["DB_CONNECTION_INFO"]["password"],
                        "port": config["DB_CONNECTION_INFO"]["port"]
                    }, outfile)
                print('db connection setting DONE')
                break
            elif i == 'n':
                return
            else:
                continue


def IEX_connection_setting(config):
    # read DB connection config files
    # noinspection PyBroadException
    try:
        with open(
                os.path.join(
                    config["DEFAULT"]["config_path"], "config_files", "IEX_CONNECTION_CONFIG.json"
                ),
                "r",
        ) as f:
            print("IEX_CONNECTION_CONFIG.json loaded...")
            iex_config_json = json.load(f)
            config["EXTERNAL_CONNECTION_INFO"]["iex_cloud_api_token"] = iex_config_json["token"]
            print('IEX connection setting DONE')

    except:
        while True:
            print("IEX connection information file does not appear to exist. Do you want to enter manually? [y/n]:")
            i = input()
            if i == 'y':
                config["EXTERNAL_CONNECTION_INFO"]["iex_cloud_api_token"] = input("iex token: ")
                print('make IEX_CONNECTION_CONFIG.json...')

                with open(os.path.join(
                        config["DEFAULT"]["config_path"], "config_files", "IEX_CONNECTION_CONFIG.json"
                ), 'w') as outfile:
                    json.dump({
                        "token": config["EXTERNAL_CONNECTION_INFO"]["iex_cloud_api_token"]
                    }, outfile)
                print('IEX connection setting DONE')
                break
            elif i == 'n':
                return
            else:
                continue


def load():
    # make config folder
    make_directory('config')
    make_directory('config/config_files')

    config = configparser.ConfigParser(strict=False)
    config.read(
        os.path.join(os.path.dirname(os.path.abspath(__file__)), "config", "config.ini")
    )

    # section
    if not config.has_section("DB_CONNECTION_INFO"):
        config.add_section("DB_CONNECTION_INFO")
    if not config.has_section("EXTERNAL_CONNECTION_INFO"):
        config.add_section("EXTERNAL_CONNECTION_INFO")

    # DEFAULT config
    config["DEFAULT"]["default_path"] = os.path.dirname(os.path.abspath(__file__))
    config["DEFAULT"]["config_path"] = os.path.join(
        config["DEFAULT"]["default_path"], "config"
    )

    # read DB connection config files
    DB_connection_setting(config)
    IEX_connection_setting(config)
    with open(
            os.path.join(config["DEFAULT"]["config_path"], "config.ini"), "w"
    ) as configfile:
        config.write(configfile)


if __name__ == '__main__':
    load()
