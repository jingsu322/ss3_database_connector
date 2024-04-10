import logzero, logging, os, boto3, io, time
from dotenv import load_dotenv, find_dotenv
from logzero import logger
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from datetime import date, datetime, timedelta
from akeneo_api_client.client import Client
from pymysql import connect, cursors


class AkeneoClient:
    def __init__(
        self,
        AKENEO_BASE_URL,
        AKENEO_CLIENT_ID,
        AKENEO_SECRET,
        AKENEO_USERNAME,
        AKENEO_PASSWORD,
    ):
        self.AKENEO_BASE_URL = AKENEO_BASE_URL
        self.AKENEO_CLIENT_ID = AKENEO_CLIENT_ID
        self.AKENEO_SECRET = AKENEO_SECRET
        self.AKENEO_USERNAME = AKENEO_USERNAME
        self.AKENEO_PASSWORD = AKENEO_PASSWORD

        self.akeneo = Client(
            self.AKENEO_BASE_URL,
            self.AKENEO_CLIENT_ID,
            self.AKENEO_SECRET,
            self.AKENEO_USERNAME,
            self.AKENEO_PASSWORD,
        )

    def fetch_data(self):
        result = self.akeneo.products.fetch_list()
        return result


class MySQLDB(object):
    def __init__(self, setting):
        self.db = connect(
            host=setting.get("DBSERVER"),
            port=int(setting.get("DBPORT")),
            user=setting.get("DBUSERNAME"),
            passwd=setting.get("DBPASSWORD"),
            db=setting.get("DB"),
            charset="utf8mb4",
            cursorclass=cursors.DictCursor,
        )

        self.cursor = self.db.cursor()

    def __del__(self):
        if self.db.open:
            self.db.close()

    def fetch_data(self, sql, params):
        if len(params) == 0:
            self.cursor.execute(sql)
        else:
            params = [None if param == "" else param for param in params]
            self.cursor.execute(sql, params)
        res = self.cursor.fetchall()
        return res


class Mage2DataSync(MySQLDB):
    def __init__(self, setting):
        MySQLDB.__init__(self, setting)

    def get_product_data(self, query):
        sql = query
        res = self.fetch_data(sql, [])
        if len(res) == 0:
            return None
        return res


def format_akeneo_result(result_list):
    output = []

    for single_item in result_list:
        item = {}

        if "identifier" in single_item.keys():
            item["identifier"] = single_item["identifier"]

        if "enabled" in single_item.keys():
            item["enabled"] = single_item["enabled"]

        if "parent" in single_item.keys():
            item["parent_identifier"] = single_item["parent"]

        if "values" in single_item.keys():
            if (
                "packaging_size" in single_item["values"].keys()
                and len(single_item["values"]["packaging_size"]) > 0
            ):
                item["packaging_size"] = single_item["values"]["packaging_size"][0][
                    "data"
                ]

            if (
                "product_name" in single_item["values"].keys()
                and len(single_item["values"]["product_name"]) > 0
            ):
                item["product_name"] = single_item["values"]["product_name"][0]["data"]

            if (
                "magento_sku" in single_item["values"].keys()
                and len(single_item["values"]["magento_sku"]) > 0
            ):
                item["magento_sku"] = single_item["values"]["magento_sku"][0]["data"]

            if (
                "seller_sku" in single_item["values"].keys()
                and len(single_item["values"]["seller_sku"]) > 0
            ):
                item["seller_sku"] = single_item["values"]["seller_sku"][0]["data"]

            if (
                "magento_config_sku" in single_item["values"].keys()
                and len(single_item["values"]["magento_config_sku"]) > 0
            ):
                item["magento_config_sku"] = single_item["values"][
                    "magento_config_sku"
                ][0]["data"]

            if (
                "master_item" in single_item["values"].keys()
                and len(single_item["values"]["master_item"]) > 0
            ):
                item["master_item"] = single_item["values"]["master_item"][0]["data"]

        output.append(item)
        # logger.info(item)

    df = pd.DataFrame(output)
    print(df.head())
    print(df.info())

    df.to_csv("../data/akeneo_output.csv", index=False)

    return df


if __name__ == "__main__":

    # Setup log
    logzero.json()
    logzero.logfile("../tmp/sync_ss3_warehouse_lots.log")
    logzero.loglevel(logzero.INFO)

    # Config
    try:
        load_dotenv(find_dotenv("config.env"))
    except ModuleNotFoundError as e:
        print("dotenv is unavailable.")
    logzero.loglevel(logging.INFO)

    AKENEO_CLIENT_ID = os.environ.get("AKENEO_CLIENT_ID")
    AKENEO_SECRET = os.environ.get("AKENEO_SECRET")
    AKENEO_USERNAME = os.environ.get("AKENEO_USERNAME")
    AKENEO_PASSWORD = os.environ.get("AKENEO_PASSWORD")
    AKENEO_BASE_URL = os.environ.get("AKENEO_BASE_URL")

    akeneo_es_host = os.environ.get("akeneo_es_host")
    akeneo_es_port = os.environ.get("akeneo_es_port")
    akeneo_es_scheme = os.environ.get("akeneo_es_scheme")
    akeneo_es_index = os.environ.get("akeneo_es_index")
    
    akeneo = AkeneoClient(
        AKENEO_BASE_URL,
        AKENEO_CLIENT_ID,
        AKENEO_SECRET,
        AKENEO_USERNAME,
        AKENEO_PASSWORD,
    )

    result = akeneo.fetch_data()
    df_akeneo = format_akeneo_result(result)