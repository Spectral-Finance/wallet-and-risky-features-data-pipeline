import os
from typing import List
import subprocess
import pandas as pd
from decimal import Decimal
import pyarrow as pa

from web3 import Web3
from spectral_data_lib.helpers.get_secrets import get_secret
from spectral_data_lib.config import settings as sdl_settings
from spectral_data_lib.data_lakehouse import DataLakehouse
from spectral_data_lib.log_manager import Logger

from src.helpers.data_transformations import add_partition_column, convert_timestamp_to_datetime
from src.helpers.get_token_metadata_transpose import TranposeTokenMetadata


class RawPipeline(object):
    """Class to fetch data from the ethereum blockchain and Save it into the data lakehouse."""

    def __init__(self):
        """Initialize the class."""
        self.logger = Logger(logger_name=f"Ethereum - Raw Pipeline Logger")
        self.data_lakehouse_connection = DataLakehouse()
        self.node_rpc_url_secret = get_secret("prod/ethereum_node/rpc_urls")
        self.transpose_api_key = get_secret("prod/transpose_api_key")["api_key"]
        self.node_rpc_urls = list(self.node_rpc_url_secret.values())
        self.retry = len(self.node_rpc_urls)
        self.timeout = 600  # Default timeout in seconds to run the subprocesses in the fetch methods.

    def fetch_blocks_and_transactions(self, start_block: int, end_block: int, node_rpc_urls: List[str], retry: int = 3):
        """Fetch blocks and transactions from the ethereum blockchain and save them to csv files.

        Args:
            start_block (int): The block number to start Fetching from.
            end_block (int): The block number to end Fetching at.
            node_rpc_urls (List[str]): A list of node rpc urls to use to fetch the data.
            retry (int): The number of times to retry the request if it fails.

        Returns:
            None
        """

        self.logger.info(f"Fetching blocks and transactions from the ethereum blockchain.")

        if Web3.HTTPProvider(node_rpc_urls[0]).isConnected():
            ethereum_etl_command = f"""
                ethereumetl export_blocks_and_transactions --start-block {start_block} --end-block {end_block} \
                --provider-uri {node_rpc_urls[0]} \
                --blocks-output data/blocks.csv --transactions-output data/transactions.csv
            """

            try:
                subprocess.run(
                    ethereum_etl_command,
                    shell=True,
                    check=True,
                    timeout=self.timeout,
                )
            except subprocess.CalledProcessError as e:
                self.logger.error(f"Error fetching blocks and transactions from the ethereum blockchain - {e}")
                if retry > 0 and len(node_rpc_urls) > 1:
                    self.logger.info(f"Retrying to fetch blocks and transactions from the ethereum blockchain.")
                    self.fetch_blocks_and_transactions(
                        start_block=start_block, end_block=end_block, node_rpc_urls=node_rpc_urls[1:], retry=retry - 1
                    )
                else:
                    raise Exception(f"Error fetching blocks and transactions from the ethereum blockchain - {e}")
            except subprocess.TimeoutExpired as e:

                self.logger.error(f"Error fetching blocks and transactions from the ethereum blockchain - {e}")
                if retry > 0 and len(node_rpc_urls) > 1:
                    self.logger.info(f"Retrying to fetch blocks and transactions from the ethereum blockchain.")
                    self.fetch_blocks_and_transactions(
                        start_block=start_block, end_block=end_block, node_rpc_urls=node_rpc_urls[1:], retry=retry - 1
                    )
                else:
                    raise Exception(
                        f"Error fetching blocks and transactions from the ethereum blockchain, none of the nodes are connected - {e}"
                    )
        else:
            self.logger.info(f"Node {node_rpc_urls[0]} is not connected. Trying next node.")
            if retry > 0 and len(node_rpc_urls) > 1:
                self.fetch_blocks_and_transactions(
                    start_block=start_block, end_block=end_block, node_rpc_urls=node_rpc_urls[1:], retry=retry - 1
                )
            else:
                raise Exception(
                    f"Error fetching blocks and transactions from the ethereum blockchain, none of the nodes are connected"
                )

    def save_blocks(self) -> pd.DataFrame:
        """Save blocks from csv files into the data lakehouse as parquet files.

        Args:
            None

        Returns:
            blocks_data_frame (pd.DataFrame): The blocks dataframe. This dataframe will be used to get the block timestamp.
        """

        self.logger.info(f"Saving blocks into the data lakehouse.")

        blocks_data_frame = pd.read_csv("data/blocks.csv")
        blocks_data_frame = convert_timestamp_to_datetime(
            data=blocks_data_frame, column="timestamp"
        )  # Needed to match with BigQuery historical data schema
        blocks_data_frame = add_partition_column(data=blocks_data_frame, column="timestamp")

        self.data_lakehouse_connection.write_parquet_table(
            table_name="ethereum_blocks",
            database_name=sdl_settings.DATA_LAKE_RAW_DATABASE,
            data=blocks_data_frame,
            source="ethereum",
            layer="raw",
            partition_columns=["date_partition"],
            mode_write="append",
        )

        self.logger.info(
            f"Blocks saved into the data lakehouse - {blocks_data_frame.shape[0]} rows - Raw Layer - Ethereum Blocks Table"
        )

        return blocks_data_frame

    def save_transactions(self):
        """Save transactions from csv files into the data lakehouse as parquet files.

        Args:
            None

        Returns:
            None
        """

        self.logger.info(f"Saving transactions into the data lakehouse.")

        transactions_data_frame = pd.read_csv("data/transactions.csv")
        receipts_data_frame = pd.read_csv("data/receipts.csv")

        # Select only the columns that we need
        receipts_columns = [
            "transaction_hash",
            "block_number",
            "cumulative_gas_used",
            "gas_used",
            "contract_address",
            "root",
            "status",
            "effective_gas_price",
        ]

        self.logger.info("Merging transactions and receipts dataframes, and renaming columns.")

        transactions_data_frame = transactions_data_frame.merge(
            receipts_data_frame[receipts_columns],
            left_on=["hash", "block_number"],
            right_on=["transaction_hash", "block_number"],
            how="inner",
        )

        columns_to_rename = {
            "cumulative_gas_used": "receipt_cumulative_gas_used",
            "gas_used": "receipt_gas_used",
            "contract_address": "receipt_contract_address",
            "root": "receipt_root",
            "status": "receipt_status",
            "effective_gas_price": "receipt_effective_gas_price",
        }

        transactions_data_frame = transactions_data_frame.rename(columns=columns_to_rename)
        transactions_data_frame = transactions_data_frame.drop(columns=["transaction_hash"])

        transactions_data_frame = convert_timestamp_to_datetime(
            data=transactions_data_frame, column="block_timestamp"
        )  # Needed to match with BigQuery historical data schema
        transactions_data_frame = add_partition_column(data=transactions_data_frame, column="block_timestamp")

        self.data_lakehouse_connection.write_parquet_table(
            table_name="ethereum_transactions",
            database_name=sdl_settings.DATA_LAKE_RAW_DATABASE,
            data=transactions_data_frame,
            source="ethereum",
            layer="raw",
            partition_columns=["date_partition"],
            mode_write="append",
        )

        self.logger.info(
            f"Transactions saved into the data lakehouse - {transactions_data_frame.shape[0]} rows - Raw Layer - Ethereum Transactions Table"
        )

    def fetch_receipts_and_logs(self, node_rpc_urls: List[str], retry: int = 3):
        """Fetch receipts and logs from the ethereum blockchain and save them to csv files.
        Receipts is necessary to join with the transactions table.

        Args:
            node_rpc_urls (List[str]): List of node rpc urls to connect to the ethereum blockchain.
            retry (int): Number of times to retry fetching receipts and logs from the ethereum blockchain.

        Returns:
            None
        """

        self.logger.info(f"Fetching logs from the ethereum blockchain.")

        if Web3.HTTPProvider(node_rpc_urls[0]).isConnected():
            ethereum_etl_command = f"""
                ethereumetl extract_csv_column --input data/transactions.csv --column hash --output data/transaction_hashes.txt && \
                ethereumetl export_receipts_and_logs --transaction-hashes data/transaction_hashes.txt \
                --provider-uri {node_rpc_urls[0]} --receipts-output data/receipts.csv --logs-output data/logs.csv
            """

            try:
                subprocess.run(ethereum_etl_command, shell=True, check=True, timeout=self.timeout)
            except subprocess.CalledProcessError as e:
                self.logger.error(f"Error fetching receipts and logs from the ethereum blockchain - {e}")
                if retry > 0 and len(node_rpc_urls) > 1:
                    self.logger.info(f"Retrying to fetch receipts and logs from the ethereum blockchain.")
                    self.fetch_receipts_and_logs(node_rpc_urls=node_rpc_urls[1:], retry=retry - 1)
                else:
                    raise Exception(f"Error Fetching receipts and logs from the ethereum blockchain - {e}")
            except subprocess.TimeoutExpired as e:
                self.logger.error(f"Error fetching receipts and logs from the ethereum blockchain - {e}")
                if retry > 0 and len(node_rpc_urls) > 1:
                    self.logger.info(f"Retrying to fetch receipts and logs from the ethereum blockchain.")
                    self.fetch_receipts_and_logs(node_rpc_urls=node_rpc_urls[1:], retry=retry - 1)
                else:
                    raise Exception(f"Error Fetching receipts and logs from the ethereum blockchain - {e}")

        else:
            self.logger.info(f"Node {node_rpc_urls[0]} is not connected. Trying next node.")
            if retry > 0 and len(node_rpc_urls) > 1:
                self.fetch_receipts_and_logs(node_rpc_urls=node_rpc_urls[1:], retry=retry - 1)
            else:
                raise Exception(
                    f"Error fetching receipts and logs from the ethereum blockchain, none of the nodes are connected"
                )

    def save_logs(self, blocks_data_frame: pd.DataFrame):
        """Save logs from csv files into the data lakehouse as parquet files.

        Args:
            blocks_data_frame (pd.DataFrame): The blocks dataframe. This dataframe will be used to get the block timestamp.

        Returns:
            None
        """

        self.logger.info(f"Saving logs into the data lakehouse.")

        logs_data_frame = pd.read_csv("data/logs.csv")

        self.logger.info("Merging logs with blocks dataframe to get the block timestamp.")

        logs_data_frame = logs_data_frame.merge(
            blocks_data_frame, how="inner", left_on="block_number", right_on="number"
        )
        logs_data_frame.rename(columns={"timestamp": "block_timestamp"}, inplace=True)

        selected_columns = [
            "log_index",
            "transaction_hash",
            "transaction_index",
            "block_hash",
            "block_number",
            "address",
            "data",
            "topics",
            "block_timestamp",
        ]

        logs_data_frame = logs_data_frame[selected_columns]

        logs_data_frame = convert_timestamp_to_datetime(data=logs_data_frame, column="block_timestamp")
        logs_data_frame = add_partition_column(data=logs_data_frame, column="block_timestamp")

        self.data_lakehouse_connection.write_parquet_table(
            table_name="ethereum_logs",
            database_name=sdl_settings.DATA_LAKE_RAW_DATABASE,
            data=logs_data_frame,
            source="ethereum",
            layer="raw",
            partition_columns=["date_partition"],
            mode_write="append",
        )

        self.logger.info(
            f"Logs saved into the data lakehouse - {logs_data_frame.shape[0]} logs saved - Raw Layer - Ethereum Logs Table"
        )

    def fetch_contracts(self, node_rpc_urls: List[str], retry: int = 3):
        """Fetch contracts from the ethereum blockchain and save them to csv files.

        Args:
            node_rpc_urls (List[str]): List of node rpc urls to connect to the ethereum blockchain.
            retry (int): Number of times to retry fetching contracts from the ethereum blockchain.

        Returns:
            None
        """

        self.logger.info(f"Fetching contracts from the ethereum blockchain using logs events.")

        if Web3.HTTPProvider(node_rpc_urls[0]).isConnected():
            ethereum_etl_command = f"""
                ethereumetl extract_csv_column --input data/receipts.csv --column contract_address --output data/contract_addresses.txt &&
                ethereumetl export_contracts --contract-addresses data/contract_addresses.txt \
                    --provider-uri {node_rpc_urls[0]} --output data/contracts.csv
            """

            try:
                subprocess.run(ethereum_etl_command, shell=True, check=True, timeout=self.timeout)
            except subprocess.CalledProcessError as e:
                self.logger.error(f"Error fetching contracts from the ethereum blockchain - {e}")
                if retry > 0 and len(node_rpc_urls) > 1:
                    self.logger.info(f"Retrying to fetch contracts from the ethereum blockchain.")
                    self.fetch_contracts(node_rpc_urls=node_rpc_urls[1:], retry=retry - 1)
                else:
                    raise Exception(f"Error Fetching contracts from the ethereum blockchain - {e}")
            except subprocess.TimeoutExpired as e:
                self.logger.error(f"Error fetching contracts from the ethereum blockchain - {e}")
                if retry > 0 and len(node_rpc_urls) > 1:
                    self.logger.info(f"Retrying to fetch contracts from the ethereum blockchain.")
                    self.fetch_contracts(node_rpc_urls=node_rpc_urls[1:], retry=retry - 1)
                else:
                    raise Exception(f"Error Fetching contracts from the ethereum blockchain - {e}")

        else:
            self.logger.info(f"Node {node_rpc_urls[0]} is not connected. Trying next node.")
            if retry > 0 and len(node_rpc_urls) > 1:
                self.fetch_contracts(node_rpc_urls=node_rpc_urls[1:], retry=retry - 1)
            else:
                raise Exception(
                    f"Error fetching contracts from the ethereum blockchain, none of the nodes are connected"
                )

    def save_contracts(self):
        """Save contracts from csv files into the data lakehouse as parquet files.

        Args:
            None

        Returns:
            None
        """

        self.logger.info(f"Saving contracts into the data lakehouse.")

        # Check if the contracts file is empty, because sometimes the logs events don't have contracts.
        if os.stat("data/contracts.csv").st_size == 0:
            self.logger.info("No contracts to save.")
        else:
            contracts_data_frame = pd.read_csv("data/contracts.csv")

            contracts_data_frame["block_timestamp"] = pd.Timestamp.now()
            contracts_data_frame = add_partition_column(data=contracts_data_frame, column="block_timestamp")

            contracts_data_frame["function_sighashes"] = contracts_data_frame["function_sighashes"].astype(str).tolist()

            self.data_lakehouse_connection.write_parquet_table(
                table_name="ethereum_contracts",
                database_name=sdl_settings.DATA_LAKE_RAW_DATABASE,
                data=contracts_data_frame,
                source="ethereum",
                layer="raw",
                partition_columns=["date_partition"],
                mode_write="append",
            )

            self.logger.info(
                f"Contracts saved into the data lakehouse - {contracts_data_frame.shape[0]} contracts saved - Raw Layer - Ethereum Contracts Table"
            )

    def fetch_tokens(self, node_rpc_urls: List[str], retry: int = 3):
        """Fetch tokens from the ethereum blockchain and save them to csv files.

        Args:
            node_rpc_urls (List[str]): List of node rpc urls to connect to the ethereum blockchain.
            retry (int): Number of times to retry fetching tokens from the ethereum blockchain..

        Returns:
            None
        """

        self.logger.info(f"Fetching tokens from the ethereum blockchain using contracts.")

        if Web3.HTTPProvider(node_rpc_urls[0]).isConnected():
            ethereum_etl_command = f"""
                ethereumetl filter_items -i data/contracts.csv -p "item['is_erc20'] or item['is_erc721']" | \
                ethereumetl extract_field -f address -o data/token_addresses.txt &&
                ethereumetl export_tokens --token-addresses data/token_addresses.txt \
                --provider-uri {node_rpc_urls[0]} --output data/tokens.csv
            """

            try:
                subprocess.run(ethereum_etl_command, shell=True, check=True, timeout=self.timeout)
            except subprocess.CalledProcessError as e:
                self.logger.error(f"Error fetching tokens from the ethereum blockchain - {e}")
                if retry > 0 and len(node_rpc_urls) > 1:
                    self.logger.info(f"Retrying to fetch tokens from the ethereum blockchain.")
                    self.fetch_tokens(node_rpc_urls=node_rpc_urls[1:], retry=retry - 1)
                else:
                    raise Exception(f"Error Fetching tokens from the ethereum blockchain - {e}")
            except subprocess.TimeoutExpired as e:
                self.logger.error(f"Error fetching tokens from the ethereum blockchain - {e}")
                if retry > 0 and len(node_rpc_urls) > 1:
                    self.logger.info(f"Retrying to fetch tokens from the ethereum blockchain.")
                    self.fetch_tokens(node_rpc_urls=node_rpc_urls[1:], retry=retry - 1)
                else:
                    raise Exception(f"Error Fetching tokens from the ethereum blockchain - {e}")

        else:
            self.logger.info(f"Node {node_rpc_urls[0]} is not connected. Trying next node.")
            if retry > 0 and len(node_rpc_urls) > 1:
                self.fetch_tokens(node_rpc_urls=node_rpc_urls[1:], retry=retry - 1)
            else:
                raise Exception(f"Error fetching tokens from the ethereum blockchain, none of the nodes are connected")

    def save_tokens(self):
        """Save tokens from csv files into the data lakehouse as parquet files.

        Args:
            None

        Returns:
            None
        """

        self.logger.info(f"Saving tokens into the data lakehouse.")

        # Check if the tokens file is empty, because sometimes the contracts don't have tokens.
        if os.stat("data/tokens.csv").st_size == 0:
            self.logger.info("No tokens to save.")
        else:
            tokens_data_frame = pd.read_csv("data/tokens.csv")

            tokens_data_frame["block_timestamp"] = pd.Timestamp.now()
            tokens_data_frame = add_partition_column(data=tokens_data_frame, column="block_timestamp")

            self.data_lakehouse_connection.write_parquet_table(
                table_name="ethereum_tokens",
                database_name=sdl_settings.DATA_LAKE_RAW_DATABASE,
                data=tokens_data_frame,
                source="ethereum",
                layer="raw",
                partition_columns=["date_partition"],
                mode_write="append",
            )

            self.logger.info(
                f"Tokens saved into the data lakehouse - {tokens_data_frame.shape[0]} tokens saved - Raw Layer - Ethereum Tokens Table"
            )

    def fetch_token_transfers(self):
        """Fetch token transfers from the ethereum blockchain and save them to csv files.
        This token_transfer is the same as the ERC20 transfer event.

        Args:
            None

        Returns:
            None
        """

        self.logger.info(f"Fetching token transfers from the ethereum blockchain using logs events.")

        ethereum_etl_command = (
            "ethereumetl extract_token_transfers --logs data/logs.csv --output data/token_transfers.csv"
        )

        try:
            subprocess.run(ethereum_etl_command, shell=True, check=True)
        except subprocess.CalledProcessError as e:
            raise Exception(f"Error Fetching token transfers from the ethereum blockchain - {e}")

    def save_token_transfers(self, blocks_data_frame: pd.DataFrame):
        """Save token transfers from csv files into the data lakehouse as parquet files.

        Args:
            blocks_data_frame (pd.DataFrame): The blocks dataframe. This dataframe will be used to get the block timestamp.

        Returns:
            None
        """

        self.logger.info(f"Saving token transfers into the data lakehouse.")

        token_transfers_data_frame = pd.read_csv("data/token_transfers.csv")

        self.logger.info("Merging token_transfer with blocks dataframe to get the block timestamp and block hash.")

        token_transfers_data_frame = token_transfers_data_frame.merge(
            blocks_data_frame, how="inner", left_on="block_number", right_on="number"
        )
        token_transfers_data_frame.rename(columns={"timestamp": "block_timestamp", "hash": "block_hash"}, inplace=True)

        selected_columns = [
            "token_address",
            "from_address",
            "to_address",
            "value",
            "transaction_hash",
            "log_index",
            "block_number",
            "block_timestamp",
            "block_hash",
        ]

        token_transfers_data_frame = token_transfers_data_frame[selected_columns]

        token_transfers_data_frame = convert_timestamp_to_datetime(
            data=token_transfers_data_frame, column="block_timestamp"
        )
        token_transfers_data_frame = add_partition_column(data=token_transfers_data_frame, column="block_timestamp")

        self.data_lakehouse_connection.write_parquet_table(
            table_name="ethereum_token_transfers",
            database_name=sdl_settings.DATA_LAKE_RAW_DATABASE,
            data=token_transfers_data_frame,
            source="ethereum",
            layer="raw",
            partition_columns=["date_partition"],
            mode_write="append",
        )

        self.logger.info(
            f"Token transfers saved into the data lakehouse - {token_transfers_data_frame.shape[0]} token transfers saved - Raw Layer - Ethereum Token Transfers Table"
        )

    def fetch_token_metadata(self) -> pd.DataFrame:
        """Fetch tokens metadata from Transpose API

        Args:
            None

        Returns:
            DataFrame: Token metadata dataframe
        """

        query = f"""
            SELECT MAX(last_refreshed) AS last_token_metadata_inserted
            FROM {sdl_settings.DATA_LAKE_RAW_DATABASE}.ethereum_tokens_metadata
            WHERE date_partition in (
                    SELECT MAX(date_partition) AS last_partition
                    FROM {sdl_settings.DATA_LAKE_RAW_DATABASE}.ethereum_tokens_metadata
                )
        """

        last_timestamp_inserted = self.data_lakehouse_connection.read_sql_query(
            query=query, database_name=sdl_settings.DATA_LAKE_RAW_DATABASE
        )["last_token_metadata_inserted"][0]

        last_timestamp_inserted = last_timestamp_inserted.strftime("%Y-%m-%d %H:%M:%S")

        token_metadata_instance = TranposeTokenMetadata(
            api_key=self.transpose_api_key, last_timestamp_inserted=last_timestamp_inserted
        )
        tokens_metadata_data_frame = token_metadata_instance.run()

        if tokens_metadata_data_frame is not None:
            self.logger.info(
                f"Tokens metadata fetched from the Transpose API - {tokens_metadata_data_frame.shape[0]} tokens metadata fetched."
            )
            return tokens_metadata_data_frame
        else:
            self.logger.info(f"No token metadata to fetch.")
            return None

    def save_token_metadata(self, tokens_metadata_data_frame: pd.DataFrame):
        """Save token metadata from csv files into the data lakehouse as parquet files.

        Args:
            tokens_metadata_data_frame (pd.DataFrame): The token metadata dataframe.

        Returns:
            None
        """

        self.logger.info(f"Saving tokens metadata into the data lakehouse.")

        if tokens_metadata_data_frame is not None:
            tokens_metadata_data_frame = add_partition_column(
                data=tokens_metadata_data_frame, column="created_timestamp"
            )

            self.data_lakehouse_connection.write_parquet_table(
                table_name="ethereum_tokens_metadata",
                database_name=sdl_settings.DATA_LAKE_RAW_DATABASE,
                data=tokens_metadata_data_frame,
                source="ethereum",
                layer="raw",
                partition_columns=["date_partition"],
                mode_write="append",
            )

            self.logger.info(
                f"Token metadata saved into the data lakehouse - {tokens_metadata_data_frame.shape[0]} token metadata saved - Raw Layer - Ethereum Tokens Metadata Table"
            )
        else:
            self.logger.info(f"No token metadata to save.")

    def fetch_traces(self, start_block: int, end_block: int, node_rpc_urls: List[str], retry: int = 3):
        """Fetch traces from the ethereum blockchain and save them to csv files.

        Args:
            start_block (int): The start block number.
            end_block (int): The end block number.
            node_rpc_urls (List[str]): The ethereum node rpc urls.
            retry (int): The number of retries in case of error.

        Returns:
            None
        """

        self.logger.info(f"Fetching traces from the ethereum blockchain.")

        if Web3.HTTPProvider(node_rpc_urls[0]).isConnected():

            ethereum_etl_command = f"""
                ethereumetl export_traces --start-block {start_block} --end-block {end_block} \
                --provider-uri {node_rpc_urls[0]} --batch-size 100 --output data/traces.csv
            """

            try:
                subprocess.run(ethereum_etl_command, shell=True, check=True, timeout=self.timeout)
            except subprocess.CalledProcessError as e:
                self.logger.warning(f"Error Fetching traces from the ethereum blockchain - {e}")
                if retry > 0 and len(node_rpc_urls) > 1:
                    self.logger.warning(f"Retrying - {retry} retries left.")
                    self.fetch_traces(
                        start_block=start_block, end_block=end_block, node_rpc_urls=node_rpc_urls[1:], retry=retry - 1
                    )
                else:
                    raise Exception(f"Error Fetching traces from the ethereum blockchain - {e}")
            except subprocess.TimeoutExpired as e:
                self.logger.warning(f"Error Fetching traces from the ethereum blockchain - {e}")
                if retry > 0 and len(node_rpc_urls) > 1:
                    self.logger.warning(f"Retrying - {retry} retries left.")
                    self.fetch_traces(
                        start_block=start_block, end_block=end_block, node_rpc_urls=node_rpc_urls[1:], retry=retry - 1
                    )
                else:
                    raise Exception(f"Error Fetching traces from the ethereum blockchain - {e}")

        else:
            self.logger.info(f"Node {node_rpc_urls[0]} is not connected. Trying next node.")
            if retry > 0 and len(node_rpc_urls) > 1:
                self.fetch_traces(
                    start_block=start_block, end_block=end_block, node_rpc_urls=node_rpc_urls[1:], retry=retry - 1
                )
            else:
                raise Exception(f"Error fetching traces from the ethereum blockchain, none of the nodes are connected")

    @staticmethod
    def change_precision_for_high_numbers(value) -> str:
        """This function is used to change the precision of the value column in the traces dataframe.

        Args:
            value (str): The value column in the traces dataframe.

        Returns:
            value (str): The value column in the traces dataframe with the precision changed.

        """
        if abs(int(value)) > 1e38:
            value = "".join([number for number in value])
            return value[:28]
        else:
            return value

    def save_traces(self, blocks_data_frame: pd.DataFrame):
        """Save traces from csv files into the data lakehouse as parquet files.

        Args:
            blocks_data_frame (pd.DataFrame): The blocks dataframe. This dataframe will be used to get the block timestamp.

        Returns:
            None
        """

        self.logger.info(f"Saving traces into the data lakehouse.")

        traces_data_frame = pd.read_csv("data/traces.csv")
        traces_data_frame["value"] = traces_data_frame["value"].apply(self.change_precision_for_high_numbers)

        self.logger.info("Merging traces with blocks dataframe to get the block timestamp and block hash.")

        traces_data_frame = traces_data_frame.merge(
            blocks_data_frame[["number", "hash", "timestamp"]], how="inner", left_on="block_number", right_on="number"
        )

        columns_to_rename = {"timestamp": "block_timestamp", "hash": "block_hash"}

        traces_data_frame.rename(columns=columns_to_rename, inplace=True)
        traces_data_frame = traces_data_frame.drop(columns=["number"])

        traces_data_frame = convert_timestamp_to_datetime(data=traces_data_frame, column="block_timestamp")
        traces_data_frame = add_partition_column(data=traces_data_frame, column="block_timestamp")

        self.data_lakehouse_connection.write_parquet_table(
            table_name="ethereum_traces",
            database_name=sdl_settings.DATA_LAKE_RAW_DATABASE,
            data=traces_data_frame,
            source="ethereum",
            layer="raw",
            partition_columns=["date_partition"],
            mode_write="append",
        )

        self.logger.info(
            f"Traces saved into the data lakehouse - {traces_data_frame.shape[0]} traces saved - Raw Layer - Ethereum Traces Table"
        )

    def check_missing_blocks(self, start_block: int, end_block: int):
        """This function is used to check if there are any missing blocks, this is a data quality check.

        Args:
            start_block (int): The start block number.
            end_block (int): The end block number.

        Returns:
            None
        """

        query_to_check_missing_blocks = f"""
        WITH numbers AS (
            SELECT *
            FROM (
                    SELECT sequence({start_block}, {end_block}) as num
                )
                CROSS JOIN UNNEST(num) as t(number)
        )
        SELECT n.number
        FROM numbers n
            LEFT JOIN ethereum_blocks e ON n.number = e.number
        WHERE e.number IS NULL
        """

        self.logger.info(f"Checking missing blocks between {start_block} and {end_block}.")

        try:
            missing_blocks = self.data_lakehouse_connection.read_sql_query(
                query=query_to_check_missing_blocks, database_name=sdl_settings.DATA_LAKE_RAW_DATABASE
            )

            if missing_blocks.shape[0] > 0:
                raise Exception(
                    f"There are missing blocks between {start_block} and {end_block} - \
                                \nMissing Blocks: {missing_blocks}"
                )

            else:
                self.logger.info(f"There are no missing blocks between {start_block} and {end_block}.")
        except Exception as e:
            raise Exception(f"Error during the process of checking missing blocks - {e}")

    def check_missing_transactions_by_block(self, start_block: int, end_block: int):
        """This function is used to check if there are any missing transactions by block, this is a data quality check.

        Args:
            start_block (int): The start block number.
            end_block (int): The end block number.

        Returns:
            None
        """

        query_to_check_missing_transactions_by_block = f"""
        -- We need to have this ranked_blocks table, because is possible that some blocks has different transaction_count depends
        -- on the node used to extract the data, in this case we need to consider the greatest value.
        WITH ranked_blocks AS (
            SELECT number,
                transaction_count,
                row_number() over(
                    partition by number
                    order by timestamp desc
                ) as block_rank
            FROM ethereum_blocks
            WHERE number BETWEEN {start_block} AND {end_block}
        )
        SELECT b.number AS block_number,
            COALESCE(t.num_transactions, 0) AS total_transactions,
            b.transaction_count AS expected_transactions,
            CASE
                WHEN COALESCE(t.num_transactions, 0) <> b.transaction_count
                OR t.block_number IS NULL THEN true ELSE false
            END AS missing_transactions
        FROM ranked_blocks as b
            LEFT JOIN (
                SELECT t.block_number,
                    COUNT(DISTINCT t.hash) AS num_transactions
                FROM ethereum_transactions as t
                INNER JOIN ethereum_traces as tr
                    ON tr.transaction_hash = t.hash
                    AND tr.block_number = t.block_number
                WHERE t.block_number BETWEEN {start_block} AND {end_block}
                GROUP BY t.block_number
            ) t ON b.number = t.block_number
        WHERE b.block_rank = 1
            AND b.transaction_count > 0
        GROUP BY b.number,
            b.transaction_count,
            t.block_number,
            t.num_transactions
        HAVING CASE
                WHEN COALESCE(t.num_transactions, 0) <> b.transaction_count
                OR t.block_number IS NULL THEN true ELSE false
            END = true
        ORDER BY b.number ASC;
        """

        self.logger.info(f"Checking missing transactions by block between {start_block} and {end_block}.")

        try:
            missing_transactions = self.data_lakehouse_connection.read_sql_query(
                query=query_to_check_missing_transactions_by_block, database_name=sdl_settings.DATA_LAKE_RAW_DATABASE
            )

            if missing_transactions.shape[0] > 0:
                raise Exception(
                    f"There are missing transactions in the blocks between {start_block} and {end_block} - \
                                \nMissing Transactions: {missing_transactions}"
                )

            else:
                self.logger.info(
                    f"There are no missing transactions in the blocks between {start_block} and {end_block}."
                )
        except Exception as e:
            raise Exception(f"Error during the process of checking missing transactions by block - {e}")

    def remove_temporary_files(self):
        """Remove temporary files.

        Args:
            None

        Returns:
            None
        """

        self.logger.info(f"Removing temporary files.")

        try:
            subprocess.run("rm -rf data", shell=True, check=True)
        except subprocess.CalledProcessError as e:
            raise Exception(f"Error removing temporary files - {e}")

    def run(self, last_block_data_lakehouse: int, last_block_ethereum_node: int) -> None:
        """ "Run the pipeline to fetch and save the data from the ethereum blockchain.

        Args:
            last_block_data_lakehouse (int): The last block saved in the data lakehouse.
            last_block_ethereum_node (int): The last block saved in the ethereum node.

        Returns:
            None
        """

        self.logger.info(f"Running the ethereum pipeline - Raw Layer.")
        self.logger.info(f"Last block saved in the data lakehouse - {last_block_data_lakehouse}")
        self.logger.info(f"Last block inserted in the ethereum node - {last_block_ethereum_node}")

        self.fetch_blocks_and_transactions(
            start_block=last_block_data_lakehouse,
            end_block=last_block_ethereum_node,
            node_rpc_urls=self.node_rpc_urls,
            retry=self.retry,
        )
        blocks_data_frame = self.save_blocks()

        self.fetch_receipts_and_logs(node_rpc_urls=self.node_rpc_urls, retry=self.retry)
        self.save_logs(blocks_data_frame=blocks_data_frame)

        self.fetch_contracts(node_rpc_urls=self.node_rpc_urls, retry=self.retry)
        self.save_contracts()

        self.fetch_tokens(node_rpc_urls=self.node_rpc_urls, retry=self.retry)
        self.save_tokens()

        self.save_transactions()

        self.fetch_token_transfers()
        self.save_token_transfers(blocks_data_frame=blocks_data_frame)

        tokens_metdata_df = self.fetch_token_metadata()
        self.save_token_metadata(tokens_metadata_data_frame=tokens_metdata_df)

        self.fetch_traces(
            start_block=last_block_data_lakehouse,
            end_block=last_block_ethereum_node,
            node_rpc_urls=self.node_rpc_urls,
            retry=self.retry,
        )
        self.save_traces(blocks_data_frame=blocks_data_frame)

        self.check_missing_blocks(start_block=last_block_data_lakehouse, end_block=last_block_ethereum_node)

        self.check_missing_transactions_by_block(
            start_block=last_block_data_lakehouse, end_block=last_block_ethereum_node
        )

        self.remove_temporary_files()

        self.logger.info(f"Ethereum pipeline finished - Raw Layer.")
