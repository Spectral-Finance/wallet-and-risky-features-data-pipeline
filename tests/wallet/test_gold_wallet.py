import json
import os
import time
from pytest import mark
from spectral_data_lib.time_manipulation import get_current_timestamp

from src.gold_process import (
    gold_document_processing,
    merge_transactions,
    gold_processing_bundle,
    merge_data_and_prices,
    get_daily_timestamp,
)

from spectral_data_lib.feature_data_documentdb.sync_mongo_connection import SyncMongoConnection


def test_gold_document_processing_normal_transaction():
    silver_doc = {
        "blockNumber": 10581792,
        "hash": "0x5bfc32998e4432f250c625bd46a9819f97057e78b1190b3912651631d6e1a3d3",
        "from": "0x475edb46f67f077eab8ac7699dc7c3a907e4c44d",
        "to": "0xfe56a0dbdad44dd14e4d560632cc842c8a13642b",
        "value": 1.0,
        "contractAddress": "",
        "type": "self-destruct",
        "gas": "0",
        "gasUsed": 0,
        "traceId": "0_0",
        "isError": False,
        "errCode": "",
        "wallet_address": "0xfe56a0dbdad44dd14e4d560632cc842c8a13642b",
        "txFee": 0.0,
        "tokenSymbol": "ETH",
        "tokenDecimal": 18,
        "timestamp": 1596390161,
    }
    gold_doc = silver_doc.copy()
    start = time.time()
    result, cum_sum = gold_document_processing(wallet_transaction_doc=gold_doc, cum_sum_tx_fee=0.0)

    end = time.time() - start
    print(f"--- {end:.10f} seconds ---")
    assert result["isError"] == 0
    assert result["transaction_sign"] == 1
    assert result != silver_doc


def test_gold_document_processing_internal_transaction():
    silver_internal_doc = {
        "blockNumber": 6838715,
        "hash": "0xeda5ce11a9cfd57ea6e71976c84d763078887f4bad87aa4ea9922402cd3b6184",
        "from": "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2",
        "to": "0x199da66fea63520d64b213ee3e7eb7cdfb650d62",
        "value": 3e18,
        "contractAddress": "",
        "type": "call",
        "gas": "2300",
        "gasUsed": 0,
        "traceId": "0",
        "isError": False,
        "errCode": "",
        "wallet_address": "0x199da66fea63520d64b213ee3e7eb7cdfb650d62",
        "txFee": 0.0,
        "tokenSymbol": "ETH",
        "tokenDecimal": 18,
        "timestamp": 1544129536,
    }

    gold_doc = silver_internal_doc.copy()
    start = time.time()
    result, cum_sum = gold_document_processing(wallet_transaction_doc=gold_doc, cum_sum_tx_fee=0.0)

    end = time.time() - start
    print(f"--- {end:.10f} seconds ---")
    assert result["isError"] == 1
    assert result["transaction_sign"] == 1
    assert result != silver_internal_doc


def test_gold_document_processing_erc20():
    silver_erc20_doc = {
        "blockNumber": 6778692,
        "hash": "0x5c2c1f150b3ccfedcfedb8b0baa2c709d3ae43a37d6bae5626765e7847a83b76",
        "nonce": "0",
        "blockHash": "0x068ff5ef072972850e943c575a78d248e71c9b91064f006224b28d68e32787eb",
        "from": "0xd64979357160e8146f6e1d805cf20437397bf1ba",
        "contractAddress": "0x89d24a6b4ccb1b6faa2625fe562bdd9a23260359",
        "to": "0x199da66fea63520d64b213ee3e7eb7cdfb650d62",
        "value": 5.4e20,
        "tokenName": "Sai Stablecoin v1.0",
        "tokenSymbol": "SAI",
        "tokenDecimal": 18,
        "transactionIndex": "81",
        "gas": "1453506",
        "gasPrice": 4000000000.0,
        "gasUsed": 953994.0,
        "cumulativeGasUsed": "4569908",
        "wallet_address": "0x199da66fea63520d64b213ee3e7eb7cdfb650d62",
        "txFee": 0.0038159760000000004,
        "isError": False,
        "timestamp": 1543271345,
    }
    gold_erc20_doc = silver_erc20_doc.copy()
    start = time.time()
    result, cum_sum = gold_document_processing(wallet_transaction_doc=gold_erc20_doc, cum_sum_tx_fee=0.0)

    end = time.time() - start
    print(f"--- {end:.10f} seconds ---")
    assert result["isError"] == 1
    assert result["transaction_sign"] == 1
    assert result != silver_erc20_doc


@mark.parametrize("wallet_address", ["0xfe56a0dbdad44dd14e4d560632cc842c8a13642b"])
def test_merge_transactions(wallet_address):
    with open(
        os.path.join(
            os.path.dirname(os.path.abspath(__file__)),
            f"../resources/wallet_transactions/{wallet_address}/silver/{wallet_address}_silver_normal.json",
        ),
        "r",
    ) as normal_file:
        normal_transaction = json.load(normal_file)
    with open(
        os.path.join(
            os.path.dirname(os.path.abspath(__file__)),
            f"../resources/wallet_transactions/{wallet_address}/silver/{wallet_address}_silver_internal.json",
        ),
        "r",
    ) as internal_file:
        internal_transaction = json.load(internal_file)

    with open(
        os.path.join(
            os.path.dirname(os.path.abspath(__file__)),
            f"../resources/wallet_transactions/{wallet_address}/silver/{wallet_address}_silver_erc20.json",
        ),
        "r",
    ) as erc20_file:
        erc20_transaction = json.load(erc20_file)
    start = time.time()
    merged = merge_transactions(
        normal_wallet_list=normal_transaction.copy(),
        internal_wallet_list=internal_transaction,
        erc20_wallet_list=erc20_transaction,
    )

    merge_path = f"../resources/wallet_transactions/{wallet_address}/silver/{wallet_address}_merged.json"

    if not os.path.exists(merge_path):
        with open(merge_path, "w") as b_file:
            json.dump(merged, b_file)

    end = time.time() - start
    print(f"--- {end:.10f} seconds ---")
    # with open(
    #         os.path.join(os.path.dirname(os.path.abspath(__file__)),
    #                      f'../resources/wallet_transactions/{wallet_address}/{wallet_address}_merged.json'),
    #         'w') as merged_file:
    #     json.dump(merged, merged_file)
    assert len(merged) == (len(normal_transaction) + len(internal_transaction) + len(erc20_transaction))
    assert merged[0]["timestamp"] < merged[1]["timestamp"]


@mark.parametrize("wallet_address", ["0xfe56a0dbdad44dd14e4d560632cc842c8a13642b"])
def test_gold_processing_bundle(wallet_address):
    with open(
        os.path.join(
            os.path.dirname(os.path.abspath(__file__)),
            f"../resources/wallet_transactions/{wallet_address}/silver/{wallet_address}_merged.json",
        ),
        "r",
    ) as merged_file:
        merged_transactions = json.load(merged_file)
    timestamp = get_current_timestamp()
    start = time.time()
    gold_dict, coin_address = gold_processing_bundle(
        wallet_transactions=merged_transactions, bundle_list=[wallet_address], threshold_timestamp=timestamp
    )
    gold_path = f"../resources/wallet_transactions/{wallet_address}/{wallet_address}_gold.json"
    end = time.time() - start
    if not os.path.exists(gold_path):
        with open(gold_path, "w") as b_file:
            json.dump([gold_dict], b_file)
    print(f"--- {end:.10f} seconds ---")
    keys = list(gold_dict.keys())
    assert keys[0] < keys[1]
    assert isinstance(gold_dict, dict)


def test_get_daily_timestamp():
    timestamp_to_get_daily = 1671331850
    daily_timestamp = get_daily_timestamp(timestamp_to_get_daily)

    assert daily_timestamp == 1671321600


@mark.parametrize("wallet_address", ["0xfe56a0dbdad44dd14e4d560632cc842c8a13642b"])
def test_get_price_data_from_db(wallet_address):
    with open(
        os.path.join(
            os.path.dirname(os.path.abspath(__file__)),
            f"../resources/wallet_transactions/{wallet_address}/{wallet_address}_merged.json",
        ),
        "r",
    ) as merged_file:
        merged_transactions = json.load(merged_file)

    gold_documents, attributes_to_query = gold_processing_bundle(
        wallet_transactions=merged_transactions,
        bundle_list=[wallet_address],
        threshold_timestamp=get_current_timestamp(),
    )

    mongo_class = SyncMongoConnection(ssh_tunnel=True)
    query_filter = {"timestamp": {"$in": list(gold_documents.keys())}}
    attributes_to_query_price = dict.fromkeys(attributes_to_query, 1)
    attributes_to_query_price["_id"] = 0
    attributes_to_query_price["timestamp"] = 1
    gold_prices_data = mongo_class.get_data(
        db_name="GOLD_HISTORICAL_WALLET_PRICES",
        collection_name="wallet_token_prices",
        filter=query_filter,
        attributes_to_project=attributes_to_query_price,
        sort_by_attr="timestamp",
    )
    with open(
        "/test/resources/wallet_transactions/data_before_fix/0xfe56a0dbdad44dd14e4d560632cc842c8a13642b/0xfe56a0dbdad44dd14e4d560632cc842c8a13642b_prices.json",
        "w",
    ) as j_fi:
        json.dump(gold_prices_data, j_fi)
    print(len(gold_prices_data))
    print("debug")
    assert len(gold_prices_data) == len(gold_documents.keys())


@mark.parametrize("wallet_address", ["0xfe56a0dbdad44dd14e4d560632cc842c8a13642b"])
def test_merge_data_and_prices(wallet_address):
    with open(
        os.path.join(
            os.path.dirname(os.path.abspath(__file__)),
            f"../resources/wallet_transactions/{wallet_address}/silver/{wallet_address}_merged.json",
        ),
        "r",
    ) as merged_file:
        merged_transactions = json.load(merged_file)

    with open(
        os.path.join(
            os.path.dirname(os.path.abspath(__file__)),
            f"../resources/wallet_transactions/{wallet_address}/{wallet_address}_prices.json",
        ),
        "r",
    ) as merged_file:
        prices_list = json.load(merged_file)

    gold_documents, attributes_to_query = gold_processing_bundle(
        wallet_transactions=merged_transactions,
        bundle_list=[wallet_address],
        threshold_timestamp=get_current_timestamp(),
    )
    gold_layer_ready = merge_data_and_prices(gold_processed_transactions=gold_documents, prices_list=prices_list)
    gold_path = f"../resources/wallet_transactions/{wallet_address}/{wallet_address}_gold_w_prices.json"
    if not os.path.exists(gold_path):
        with open(gold_path, "w") as b_file:
            json.dump(gold_layer_ready, b_file)
    count_with_no_keys = []
    count_with_empty_prices = []

    for i in range(len(gold_layer_ready)):
        if "eth_price" not in gold_layer_ready[i].keys():
            count_with_no_keys.append(i)
        else:
            if not gold_layer_ready[i].get("eth_price") or gold_layer_ready[i].get("eth_price") <= 0:
                count_with_empty_prices.append(i)

    assert len(count_with_empty_prices) == 0
    assert len(count_with_no_keys) == 0
