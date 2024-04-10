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


def get_inventory(mage2_setting, query):
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
    df_format = format_products(df)

    print(df_format.head())
    print(df_format.info())
    # df_format.to_csv("../data/get_inventory.csv", index=False)

    return df_format


def format_products(df):
    dt_lst = df.filter(regex="(created_at)|(updated_at)|(date)").columns
    print(dt_lst)
    for dt_col in dt_lst:
        print(dt_col)
        for dt_col in dt_lst:
            # df[dt_col] = df[dt_col].replace(
            #         regex="9999-01-01 00:00:00", value="1688-01-01 00:00:00"
            # )
            df[dt_col] = pd.to_datetime(df[dt_col], errors="coerce")
            # df[dt_col] = df[dt_col].fillna("1688-01-01 00:00:00")
            df[dt_col] = df[dt_col].dt.strftime("%Y-%m-%d %H:%M:%S")

    # df.to_csv("../data/format_products.csv", index=False)
    return df


def get_warehouse_lots(df_akeneo, df_ss3):
    df = df_akeneo.merge(
        df_ss3, left_on="identifier", right_on="product_identifier", how="left"
    )
    print(df.head())
    print(df.info())
    df.to_csv("../data/get_warehouse_lots.csv", index=False)
    return df


def write_to_s3(df, dest_bucket, dest_prefix, file_name):

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
    logzero.logfile("../tmp/sync_ss3_warehouse_lots.log")
    logzero.loglevel(logzero.INFO)

    # Config
    try:
        # dotenv_path = find_dotenv("/home/bibo.wang/jingscripts/ss3_database_connector/src/config.env")
        dotenv_path = find_dotenv("config.env")
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

    s3_client = boto3.client(
        "s3",
        region_name=os.environ.get("region_name"),
        aws_access_key_id=os.environ.get("aws_access_key_id"),
        aws_secret_access_key=os.environ.get("aws_secret_access_key"),
    )
    dest_bucket = "io-data-lake"
    dest_prefix_ = "jsu/ss3_inventory/"

    # Queries
    main_query = """
    SELECT 
        warehouse_products.warehouse_product_id AS warehouse_product_id,
        warehouse_products.warehouse_id AS warehouse_id,
        warehouse_products.product_identifier AS product_identifier,
        warehouse_products.safety_qty AS safety_qty,
        warehouse_products.max_qty AS max_qty,
        warehouse_products.lead_time AS lead_time,
        warehouse_products.lead_time_uom AS lead_time_uom,
        warehouse_products.on_hand_qty AS on_hand_qty,
        warehouse_products.on_order_qty AS on_order_qty,
        warehouse_products.in_transit_qty AS in_transit_qty,
        warehouse_products.on_hold_qty AS on_hold_qty,
        warehouse_products.available_qty AS available_qty,
        warehouse_products.update_date AS update_date,
        warehouse_products.restocking_date AS restocking_date,
        warehouse_products.send_safety_notice AS send_safety_notice,
        warehouse_products.allow_backorders AS allow_backorders,
        warehouse_products.seller_id AS seller_id,
        warehouse_products.team_id AS team_id,
        warehouse_products.factory_code AS factory_code,
        warehouse_products.imported_seller_sku AS imported_seller_sku,
        warehouses_1.warehouse_code AS warehouses_warehouse_code,
        warehouses_1.warehouse_name AS warehouses_warehouse_name,
        warehouses_1.active AS warehouses_active,
        warehouses_1.warehouse_type AS warehouses_warehouse_type
    FROM warehouse_products
        LEFT OUTER JOIN warehouses AS warehouses_1 ON warehouse_products.warehouse_id = warehouses_1.warehouse_id 
    """

    warehouse_product_available_lots = """
    SELECT 
        warehouse_product_available_lots.warehouse_product_id,
        warehouse_product_available_lots.lot_no AS available_lots_lot_no,
        warehouse_product_available_lots.lot_qty AS available_lots_lot_qty,
        warehouse_product_available_lots.created_at AS available_lots_created_at,
        warehouse_product_available_lots.updated_at AS available_lots_updated_at,
        warehouse_product_available_lots.previous_expiration_date AS available_lots_previous_expiration_date,
        warehouse_product_available_lots.earliest_ir_date AS available_lots_earliest_ir_date,
        warehouse_product_available_lots.expiration_date AS available_lots_expiration_date,
        warehouse_product_available_lots.aging AS available_lots_aging
    FROM warehouse_product_available_lots
    """

    warehouse_product_intransit_lots = """
    SELECT
        warehouse_product_intransit_lots.warehouse_product_id,
        warehouse_product_intransit_lots.lot_no AS intransit_lots_lot_no,
        warehouse_product_intransit_lots.created_at AS intransit_lots_created_at,
        warehouse_product_intransit_lots.updated_at AS intransit_lots_updated_at,
        warehouse_product_intransit_lots.lot_qty AS intransit_lots_lot_qty,
        warehouse_product_intransit_lots.previous_expiration_date AS intransit_lots_previous_expiration_date,
        warehouse_product_intransit_lots.earliest_ir_date AS intransit_lots_earliest_ir_date,
        warehouse_product_intransit_lots.expiration_date AS intransit_lots_expiration_date,
        warehouse_product_intransit_lots.aging AS intransit_lots_aging
    FROM warehouse_product_intransit_lots
    """

    warehouse_product_onhand_lots = """
    SELECT
        warehouse_product_onhand_lots.warehouse_product_id,
        warehouse_product_onhand_lots.lot_no AS onhand_lots_lot_no,
        warehouse_product_onhand_lots.created_at AS onhand_lots_created_at,
        warehouse_product_onhand_lots.updated_at AS onhand_lots_updated_at,
        warehouse_product_onhand_lots.lot_qty AS onhand_lots_lot_qty,
        warehouse_product_onhand_lots.previous_expiration_date AS onhand_lots_previous_expiration_date,
        warehouse_product_onhand_lots.earliest_ir_date AS onhand_lots_earliest_ir_date,
        warehouse_product_onhand_lots.expiration_date AS onhand_lots_expiration_date,
        warehouse_product_onhand_lots.aging AS onhand_lots_aging
    FROM warehouse_product_onhand_lots
    """

    warehouse_product_onhold_lots = """
    SELECT
        warehouse_product_onhold_lots.warehouse_product_id,
        warehouse_product_onhold_lots.lot_no AS onhold_lots_lot_no,
        warehouse_product_onhold_lots.created_at AS onhold_lots_created_at,
        warehouse_product_onhold_lots.updated_at AS onhold_lots_updated_at,
        warehouse_product_onhold_lots.lot_qty AS onhold_lots_lot_qty,
        warehouse_product_onhold_lots.previous_expiration_date AS onhold_lots_previous_expiration_date,
        warehouse_product_onhold_lots.earliest_ir_date AS onhold_lots_earliest_ir_date,
        warehouse_product_onhold_lots.expiration_date AS onhold_lots_expiration_date,
        warehouse_product_onhold_lots.aging AS onhold_lots_aging
    FROM warehouse_product_onhold_lots
    """

    warehouse_product_onorder_lots = """
    SELECT
        warehouse_product_onorder_lots.warehouse_product_id,
        warehouse_product_onorder_lots.lot_no AS onorder_lots_lot_no,
        warehouse_product_onorder_lots.created_at AS onorder_lots_created_at,
        warehouse_product_onorder_lots.updated_at AS onorder_lots_updated_at,
        warehouse_product_onorder_lots.lot_qty AS onorder_lots_lot_qty,
        warehouse_product_onorder_lots.previous_expiration_date AS onorder_lots_previous_expiration_date,
        warehouse_product_onorder_lots.earliest_ir_date AS onorder_lots_earliest_ir_date,
        warehouse_product_onorder_lots.expiration_date AS onorder_lots_expiration_date,
        warehouse_product_onorder_lots.aging AS onorder_lots_aging
    FROM warehouse_product_onorder_lots
    """

    # Run
    df_akeneo = pd.read_csv("../data/akeneo_output.csv")
    df_ss3 = get_inventory(ss3_setting, main_query)
    # df_ss3 = pd.read_csv("../data/ss3_output.csv")

    # Write data
    df = get_warehouse_lots(df_akeneo, df_ss3)
    df = df[
        [
            "identifier",
            "parent_identifier",
            "enabled",
            "product_name",
            "magento_sku",
            "seller_sku",
            "magento_config_sku",
            "master_item",
            "warehouse_product_id",
            "safety_qty",
            "max_qty",
            "lead_time",
            "lead_time_uom",
            "on_hand_qty",
            "on_order_qty",
            "in_transit_qty",
            "on_hold_qty",
            "available_qty",
            "update_date",
            "restocking_date",
            "send_safety_notice",
            "allow_backorders",
            "seller_id",
            "team_id",
            "factory_code",
            "imported_seller_sku",
            "warehouses_warehouse_code",
            "warehouses_warehouse_name",
            "warehouses_active",
            "warehouses_warehouse_type",
        ]
    ]
    df["warehouse_product_id"] = df["warehouse_product_id"].fillna(999999999)
    df["warehouse_product_id"] = df["warehouse_product_id"].astype(int)
    write_to_s3(
        df, dest_bucket, dest_prefix=dest_prefix_ + "data/", file_name="data.parquet"
    )

    df_available_lots = get_inventory(ss3_setting, warehouse_product_available_lots)
    df_intransit_lots = get_inventory(ss3_setting, warehouse_product_intransit_lots)
    df_onhand_lots = get_inventory(ss3_setting, warehouse_product_onhand_lots)
    df_onhold_lots = get_inventory(ss3_setting, warehouse_product_onhold_lots)
    df_onorder_lots = get_inventory(ss3_setting, warehouse_product_onorder_lots)

    write_to_s3(
        df_available_lots,
        dest_bucket,
        dest_prefix=dest_prefix_ + "available_lots/",
        file_name="available_lots.parquet",
    )
    write_to_s3(
        df_intransit_lots,
        dest_bucket,
        dest_prefix=dest_prefix_ + "intransit_lots/",
        file_name="intransit_lots.parquet",
    )
    write_to_s3(
        df_onhand_lots,
        dest_bucket,
        dest_prefix=dest_prefix_ + "onhand_lots/",
        file_name="onhand_lots.parquet",
    )
    write_to_s3(
        df_onhold_lots,
        dest_bucket,
        dest_prefix=dest_prefix_ + "onhold_lots/",
        file_name="onhold_lots.parquet",
    )
    write_to_s3(
        df_onorder_lots,
        dest_bucket,
        dest_prefix=dest_prefix_ + "onorder_lots/",
        file_name="onorder_lots.parquet",
    )
