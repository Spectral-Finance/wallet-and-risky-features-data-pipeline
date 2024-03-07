import json
import os
import re
import time

from pytest import mark
from spectral_data_lib.feature_data_documentdb.sync_mongo_connection import SyncMongoConnection

from src.new_silver import (
    silver_processing,
    ATTRIBUTES_TO_FILTER_ERC20_TRANSACTION,
    ATTRIBUTES_TO_FILTER_INTERNAL_TRANSACTION,
    ATTRIBUTES_TO_FILTER_NORMAL_TRANSACTION,
    snake_case,
)

SNAKE_CASE_TEST = re.compile(r"^[a-z][a-z0-9]+(_[a-z0-9]+)*$")


# If you don't have this file saved to run it you need to connect by ssh
@mark.parametrize("wallet_address", ["0xfe56a0dbdad44dd14e4d560632cc842c8a13642b"])
def test_silver_transform_normal_transactions(wallet_address, read_map_db):
    attributes_to_query = ATTRIBUTES_TO_FILTER_NORMAL_TRANSACTION
    path = f"../resources/wallet_transactions/{wallet_address}/bronze/{wallet_address}_normal.json"
    if not os.path.exists(path):
        mongo_class = SyncMongoConnection(ssh_tunnel=True)
        query_filter = {"wallet_address": wallet_address}
        map_db = read_map_db
        db_data = map_db["etherscan_data_db"]
        bronze_normal_transactions_data = mongo_class.get_data(
            db_name=db_data["bronze_db_name"],
            collection_name=db_data["normal_collection"],
            filter=query_filter,
            attributes_to_project=attributes_to_query,
            sort_by_attr="timestamp",
            sort_order=-1,
        )
        with open(path, "w") as b_file:
            json.dump(bronze_normal_transactions_data, b_file)
    else:
        with open(os.path.join(os.path.dirname(os.path.abspath(__file__)), path), "r") as bronze_file:
            bronze_normal_transactions_data = json.load(bronze_file)
    print(bronze_normal_transactions_data[0].keys())
    silver_data = silver_processing(bronze_normal_transactions_data, "normal")

    result = [x for x in silver_data]
    print(result[0].keys())
    for key in result[0].keys():
        assert SNAKE_CASE_TEST.match(key)

    silver_path = f"../resources/wallet_transactions/{wallet_address}/silver/{wallet_address}_silver_normal.json"
    if not os.path.exists(silver_path):
        with open(silver_path, "w") as b_file:
            json.dump(result, b_file)

    assert len(result) == len(bronze_normal_transactions_data)
    assert isinstance(result, list)


@mark.parametrize("wallet_address", ["0xfe56a0dbdad44dd14e4d560632cc842c8a13642b"])
def test_silver_transform_internal_transactions(wallet_address, read_map_db):
    attributes_to_query = ATTRIBUTES_TO_FILTER_INTERNAL_TRANSACTION
    path = f"../resources/wallet_transactions/{wallet_address}/bronze/{wallet_address}_internal.json"
    if not os.path.exists(path):
        mongo_class = SyncMongoConnection(ssh_tunnel=True)
        query_filter = {"wallet_address": wallet_address}
        map_db = read_map_db
        db_data = map_db["etherscan_data_db"]
        bronze_internal_transactions_data = mongo_class.get_data(
            db_name=db_data["bronze_db_name"],
            collection_name=db_data["internal_collection"],
            filter=query_filter,
            attributes_to_project=attributes_to_query,
            sort_by_attr="timestamp",
        )
        with open(path, "w") as b_file:
            json.dump(bronze_internal_transactions_data, b_file)
    else:
        with open(os.path.join(os.path.dirname(os.path.abspath(__file__)), path), "r") as bronze_file:
            bronze_internal_transactions_data = json.load(bronze_file)

    silver_data = silver_processing(bronze_internal_transactions_data, "internal")
    result = [x for x in silver_data]
    for key in result[0].keys():
        assert SNAKE_CASE_TEST.match(key)

    assert len(result) == len(bronze_internal_transactions_data)
    assert isinstance(result, list)

    silver_path = f"../resources/wallet_transactions/{wallet_address}/silver/{wallet_address}_silver_internal.json"
    if not os.path.exists(silver_path):
        with open(silver_path, "w") as b_file:
            json.dump(result, b_file)


@mark.parametrize("wallet_address", ["0xfe56a0dbdad44dd14e4d560632cc842c8a13642b"])
def test_silver_transform_erc_20_transactions(wallet_address, read_map_db):
    attributes_to_query = ATTRIBUTES_TO_FILTER_ERC20_TRANSACTION

    path = f"../resources/wallet_transactions/{wallet_address}/bronze/{wallet_address}_erc20.json"
    if not os.path.exists(path):
        mongo_class = SyncMongoConnection(ssh_tunnel=True)
        query_filter = {"wallet_address": wallet_address}
        map_db = read_map_db
        db_data = map_db["etherscan_data_db"]
        bronze_erc_20_transactions_data = mongo_class.get_data(
            db_name=db_data["bronze_db_name"],
            collection_name=db_data["erc20_collection"],
            filter=query_filter,
            attributes_to_project=attributes_to_query,
            sort_by_attr="timestamp",
        )
        with open(path, "w") as b_file:
            json.dump(bronze_erc_20_transactions_data, b_file)
    else:
        with open(os.path.join(os.path.dirname(os.path.abspath(__file__)), path), "r") as bronze_file:
            bronze_erc_20_transactions_data = json.load(bronze_file)
    start = time.time()
    silver_data = silver_processing(bronze_erc_20_transactions_data, "erc20")

    result = [x for x in silver_data]
    end = time.time() - start
    print(f"--- {end:.10f} seconds ---")
    for key in result[0].keys():
        assert SNAKE_CASE_TEST.match(key)

    silver_path = f"../resources/wallet_transactions/{wallet_address}/silver/{wallet_address}_silver_erc20.json"

    if not os.path.exists(silver_path):
        with open(silver_path, "w") as b_file:
            json.dump(result, b_file)

    assert len(result) == len(bronze_erc_20_transactions_data)
    assert isinstance(result, list)


def test_snake_case():
    document = {
        "blockNumber": 16349193,
        "timeStamp": "1673025623",
        "hash": "0xff1a7477b4a5f6872f34861ff63f585ea5459925a2a97c36e9a9ed3cd216db2f",
        "from": "0xfe56a0dbdad44dd14e4d560632cc842c8a13642b",
        "contractAddress": "0x9c78ee466d6cb57a4d01fd887d2b5dfb2d46288f",
        "to": "0xa872d244b8948dfd6cb7bd19f79e7c1bfb7db4a0",
        "value": "30000000000000000001",
        "tokenName": "Must",
        "tokenSymbol": "MUST",
        "tokenDecimal": "18",
        "gas": "190961",
        "gasPrice": "35098152745",
        "gasUsed": "123739",
        "cumulativeGasUsed": "12780496",
        "wallet_address": "0xfe56a0dbdad44dd14e4d560632cc842c8a13642b",
    }

    for key in ["gasPrice", "gasUsed", "tokenSymbol", "contractAddress"]:
        snake_case(key, document)

    assert "gas_price" in document.keys()
    for key in ["gasPrice", "gasUsed", "tokenSymbol", "contractAddress"]:
        assert key not in document.keys()
