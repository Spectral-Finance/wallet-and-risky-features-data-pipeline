import json
import os

from src.preparation.etherscan.balance_calculation import calculate_new_balance, SQL_WALLET_BALANCE_SCHEMA


def test_calculate_new_balance_normal_transaction_already_balance():
    wallet_address = "0xfe56a0dbdad44dd14e4d560632cc842c8a13642b"
    previous_balance = {
        "timestamp": 1596390151,
        "eth_balance": 1.2,
        "gas_fee_cum_sum": 0.3,
        "chain": {
            "ethereum": {"coin": str, "balance": float},
            "polygon": {"coin": str, "balance": float},
            "avalanche": {"coin": str, "balance": float},
        },
    }
    with open(
        os.path.join(
            os.path.dirname(os.path.abspath(__file__)),
            f"../resources/wallet_transactions/{wallet_address}/silver/{wallet_address}_silver_normal.json",
        ),
        "r",
    ) as normal_file:
        normal_transaction = json.load(normal_file)

    schema_sql_wallet_balance = SQL_WALLET_BALANCE_SCHEMA

    calculate_new_balance()


def test_calculate_new_balance_erc20_transaction_already_balance():
    gold_document_erc20 = {
        "blockNumber": 10581792,
        "hash": "0x5bfc32998e4432f250c625bd46a9819f97057e78b1190b3912651631d6e1a3d3",
        "from": "0x475edb46f67f077eab8ac7699dc7c3a907e4c44d",
        "to": "0xfe56a0dbdad44dd14e4d560632cc842c8a13642b",
        "value": 0.0,
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
        "transaction_sign": 0,
        "tx_fee_paid": 0.0,
        "curr_val": 0.0,
    }

    calculate_new_balance()


def test_calculate_new_balance_internal_transaction_already_balance():
    gold_document_erc20 = {
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
        "transaction_sign": 1,
        "tx_fee_paid": 0,
        "curr_val": 3.0,
    }
    calculate_new_balance()


def test_calculate_new_balance_internal_transaction_first_transaction():
    gold_document_eth = {
        "blockNumber": 10581792,
        "hash": "0x5bfc32998e4432f250c625bd46a9819f97057e78b1190b3912651631d6e1a3d3",
        "from": "0x475edb46f67f077eab8ac7699dc7c3a907e4c44d",
        "to": "0xfe56a0dbdad44dd14e4d560632cc842c8a13642b",
        "value": 0.7,
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
        "transaction_sign": 0,
        "eth_price": 1,
        "tx_fee_paid": 0.0,
        "curr_val": 0.0,
    }
    calculate_new_balance()
