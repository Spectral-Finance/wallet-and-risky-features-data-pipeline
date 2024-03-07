import sys
import logging as logger
from datetime import datetime

import awswrangler as wr
import requests
import boto3
import json

from airflow.models import Variable
from airflow.hooks.base_hook import BaseHook

logger.basicConfig(format="%(asctime)s - %(levelname)s - %(message)s", level=logger.INFO, stream=sys.stdout)


def get_last_block_data_lakehouse(data_lake_database: str) -> int:
    """Function to get last block from the data_lakehouse.

    Args:
        data_lake_database (str): Data lake database name.

    Returns:
        int: Last block
    """

    logger.info("Getting last block from the Data Lakehouse.")

    conn = BaseHook.get_connection("aws_default")

    session = boto3.Session(
        aws_access_key_id=conn.extra_dejson.get("aws_access_key_id"),
        aws_secret_access_key=conn.extra_dejson.get("aws_secret_access_key"),
        region_name=conn.extra_dejson.get("region_name"),
    )

    query = f"""
        SELECT MAX(number) AS last_block
        FROM {data_lake_database}.ethereum_blocks
        WHERE date_partition in (
                SELECT MAX(date_partition) AS last_partition
                FROM {data_lake_database}.ethereum_blocks
            )
    """

    last_block = wr.athena.read_sql_query(query, database=data_lake_database, boto3_session=session).iloc[0][
        "last_block"
    ]

    logger.info(f"Last block from the Data Lakehouse - Last block: {last_block}")

    return last_block


def get_last_block_from_ethereum_blockchain() -> int:
    """Function to get last block from the ethereum blockchain.

    Returns:
        int: Last block
    """

    logger.info("Getting last block from the Ethereum Blockchain.")

    url = Variable.get("ethereum_node_rpc_url")

    payload = json.dumps({"method": "eth_blockNumber", "params": [], "id": 1, "jsonrpc": "2.0"})

    headers = {"Content-Type": "application/json"}

    try:

        response = requests.request("POST", url, headers=headers, data=payload)

        last_block = int(response.json()["result"], 16)

        logger.info(f"Last block from the Ethereum Blockchain - Last block: {last_block}")

    except Exception as e:
        logger.error(f"Error getting last block from the Ethereum Blockchain - Error: {e}")

    return last_block
