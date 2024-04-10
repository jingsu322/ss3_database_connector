import logzero, logging, os, boto3, io, time
from dotenv import load_dotenv, find_dotenv
from logzero import logger
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from datetime import date, datetime, timedelta
from akeneo_api_client.client import Client
from sshtunnel import SSHTunnelForwarder
from pymysql import connect, cursors
import numpy as np


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


def get_products(mage2_setting, query):
    logger.info(f"{time.strftime('%X')}: Retrieve data from Mage2.")
    with SSHTunnelForwarder(
        (mage2_setting["SSHSERVER"], mage2_setting["SSHSERVERPORT"]),
        ssh_username=mage2_setting["SSHUSERNAME"],
        ssh_pkey=mage2_setting.get("SSHPKEY", None),
        ssh_password=mage2_setting.get("SSHPASSWORD", None),
        remote_bind_address=(
            mage2_setting["REMOTEBINDSERVER"],
            mage2_setting["REMOTEBINDSERVERPORT"],
        ),
        local_bind_address=(
            mage2_setting["LOCALBINDSERVER"],
            mage2_setting["LOCALBINDSERVERPORT"],
        ),
    ) as server:
        try:
            mage2DataSync = Mage2DataSync(mage2_setting)
            products = mage2DataSync.get_product_data(query)
            logger.info(
                f"{time.strftime('%X')}: Retrieve data from Mage2 and completed."
            )
            del mage2DataSync

        except Exception as e:
            log = "Failed to connect ssh server with error: %s" % str(e)
            logger.exception(log)
        server.stop()
        server.close()

    df = pd.DataFrame(products)
    print(df.head())
    print(df.info())
    df.to_csv("../data/ss3_output.csv", index=False)

    return df


def get_merged_df(df_akeneo, df_ss3):
    df = df_akeneo.merge(
        df_ss3, left_on="identifier", right_on="product_identifier", how="left"
    )
    print(df.head())
    print(df.info())
    df.to_csv("../data/sync_agreement_type.csv", index=False)
    return df


def sync_to_s3(df, dest_bucket, dest_prefix, file_name):

    logger.info(f"{time.strftime('%X')}: Start to write data to S3.")
    table = pa.Table.from_pandas(df)
    buffer = io.BytesIO()
    pq.write_table(table, buffer)
    buffer.seek(0)
    s3_client.upload_fileobj(buffer, dest_bucket, dest_prefix + file_name)

    logger.info(f"{time.strftime('%X')}: Write data to S3 completed.")


if __name__ == "__main__":

    # Setup log
    logzero.json()
    logzero.logfile("../tmp/sync_agreement_type.log")
    # Set a minimum loglevel
    logzero.loglevel(logzero.INFO)

    # Config
    try:
        dotenv_path = find_dotenv("/home/mike.wang/datawaldv2/ss3_database/src/config.env")
        # dotenv_path = find_dotenv("/Users/jsu_m3/Desktop/GwiWorkSpace/ss3_database_connector/src/config.env")
        if dotenv_path:
            # Load the environment variables from 'config.env', overriding any existing variables
            print(f"'config.env' found at: {dotenv_path}")
            load_dotenv(dotenv_path, override=True)
            print("Environment variables loaded from 'config.env'.")
            for k, v in os.environ.items():
                print(f"{k}: {v}")
        else:
            print("Could not find 'config.env'.")
        # load_dotenv(find_dotenv("config.env"))
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

    ss3_setting = {
        "SSHSERVER": os.environ.get("SSHSERVER"),
        "SSHSERVERPORT": int(os.environ.get("SSHSERVERPORT")),
        "SSHUSERNAME": os.environ.get("SSHUSERNAME"),
        "SSHPKEY": os.environ.get("SSHPKEY"),
        "REMOTEBINDSERVER": os.environ.get("REMOTEBINDSERVER"),
        "REMOTEBINDSERVERPORT": int(os.environ.get("REMOTEBINDSERVERPORT")),
        "LOCALBINDSERVER": os.environ.get("LOCALBINDSERVER"),
        "LOCALBINDSERVERPORT": int(os.environ.get("LOCALBINDSERVERPORT")),
        "DBSERVER": os.environ.get("DBSERVER"),
        "DBUSERNAME": os.environ.get("DBUSERNAME"),
        "DBPASSWORD": os.environ.get("DBPASSWORD"),
        "DB": os.environ.get("DB"),
        "DBPORT": int(os.environ.get("DBPORT")),
    }

    query = """
    SELECT 
    product_terms.*,
    program_types.display_group as agreement_type
    FROM product_terms 
    LEFT JOIN program_types ON product_terms.program_type_id = program_types.program_type_id 
    WHERE product_terms.active = 1 
    AND product_terms.start_date <= CURRENT_TIMESTAMP 
    AND CURRENT_TIMESTAMP <= product_terms.end_date
    """

    s3_client = boto3.client(
        "s3",
        region_name=os.environ.get("region_name"),
        aws_access_key_id=os.environ.get("aws_access_key_id"),
        aws_secret_access_key=os.environ.get("aws_secret_access_key"),
    )
    dest_bucket = "io-data-lake"
    dest_prefix = "jsu/ss3_agreement_type/"
    file_name = "ss3_agreement_type.parquet"

    # # Run
    akeneo = AkeneoClient(
        AKENEO_BASE_URL,
        AKENEO_CLIENT_ID,
        AKENEO_SECRET,
        AKENEO_USERNAME,
        AKENEO_PASSWORD,
    )

    result = akeneo.fetch_data()

    # df_akeneo = format_akeneo_result(result)
    df_akeneo = pd.read_csv("../data/akeneo_output.csv")

    df_ss3 = get_products(ss3_setting, query)
    df_ss3["proposed_date"] = df_ss3["proposed_date"].dt.strftime("%Y-%m-%d")
    df_ss3["start_date"] = df_ss3["start_date"].dt.strftime("%Y-%m-%d")
    df_ss3["end_date"] = df_ss3["end_date"].dt.strftime("%Y-%m-%d")
    print(df_ss3.info())
    # df_ss3 = pd.read_csv("../data/ss3_output.csv")

    df = get_merged_df(df_akeneo, df_ss3)
    df = df[
        [
            "identifier",
            "enabled",
            "product_name",
            "magento_sku",
            "active",
            "is_online",
            "agreement_type",
            "start_date",
            "end_date",
        ]
    ]
    print(df.info())
    print(df.head())

    sync_to_s3(df, dest_bucket, dest_prefix, file_name)
