import numpy as np
import awswrangler as wr
import concurrent.futures
from itertools import product

from config import settings
from spectral_data_lib.log_manager import Logger
from spectral_data_lib.config import settings as sdl_settings
from spectral_data_lib.data_lakehouse import DataLakehouse
from src.helpers.files import read_sql_file
from src.helpers.athena import write_data_into_datalake_using_ctas, write_data_into_datalake_using_ctas_by_chunks

# from src.schemas.analytics_layer import ETHEREUM_TABLES_SCHEMA


class AnalyticsPipeline(object):
    """Class to create a analytics pipeline"""

    def __init__(self, table_name: str) -> None:
        """Constructor for the class

        Args:
            table_name (str): Table name

        Returns:
            None
        """
        self.logger = Logger(logger_name=f"Ethereum - Analytics Pipeline Logger")
        self.data_lake_layer = "analytics"
        self.table_name = table_name
        self.target_data_lake_database = sdl_settings.DATA_LAKE_ANALYTICS_DATABASE
        self.data_lake_bucket = sdl_settings.DATA_LAKE_BUCKET_S3
        self.data_source = "ethereum"
        self.env = settings.ENV
        self.data_lakehouse_connection = DataLakehouse()

    def get_last_block_from_table(self) -> int:
        """Function to get the last block from the table in the data lakehouse

        Args:
            None

        Returns:
            int: Last timestamp
        """

        table_exist = wr.catalog.does_table_exist(database=self.target_data_lake_database, table=self.table_name)

        if table_exist:

            self.logger.info(f"Table {self.table_name} exist - Incremental ingestion.")

            query_to_get_last_block_inserted = f"""
            SELECT MAX(column) AS last_block_inserted
            FROM {self.target_data_lake_database}.{self.table_name}
            WHERE date_partition in (
                    SELECT MAX(date_partition) AS last_partition
                    FROM {self.target_data_lake_database}.{self.table_name}
                )
            """

            last_block_inserted = (
                self.data_lakehouse_connection.read_sql_query(
                    query=query_to_get_last_block_inserted.replace("column", "block_number"),
                    database_name=self.target_data_lake_database,
                )["last_block_inserted"][0]
                + 1
            )

        else:  # Default values for the first time

            self.logger.info(f"Table {self.table_name} does not exist - Full ingestion.")

            if self.table_name == "ethereum_normal_transactions":
                last_block_inserted = 46147
            elif self.table_name == "ethereum_erc20_transactions":
                last_block_inserted = 447767
            else:
                last_block_inserted = 0

        return last_block_inserted

    def run(self, **kwargs) -> None:
        """Function to run the pipeline

        Args:
            **kwargs: Keyword arguments
        """
        self.logger.info("Analytics pipeline started")

        sql_file_path = f"src/pipelines/{self.data_lake_layer}/transformations/{self.table_name}.sql"

        self.logger.info(
            f"Running data ingestion - Data Source: {self.data_source} - Table: {self.table_name} - Layer: {self.data_lake_layer}"
        )

        sql_query = read_sql_file(file_path=sql_file_path)

        last_block = self.get_last_block_from_table()

        self.logger.info(
            f"Last block: {last_block} - Data Source: {self.data_source} - Table: {self.table_name} - Layer: {self.data_lake_layer}"
        )

        if self.table_name == "ethereum_wallet_transactions":

            addresses_partitions = list(
                map("".join, product("0123456789abcdef", repeat=2))
            )  # generate all the possible addresses partitions (256)
            addresses_partitions_chunks = np.array_split(addresses_partitions, 10)

            # We are using a ProcessPoolExecutor to run the queries in parallel.
            # We are using a CTAS to write the data into the data lakehouse by chunks.
            with concurrent.futures.ProcessPoolExecutor() as executor:
                futures = []
                for addresses_partition_chunk in addresses_partitions_chunks:

                    addresses_partition_chunk = tuple(addresses_partition_chunk)

                    futures.append(
                        executor.submit(
                            write_data_into_datalake_using_ctas_by_chunks,
                            sql_query=sql_query,
                            chunk=addresses_partition_chunk,
                            filter_value=last_block,
                            env=self.env,
                            data_lake_layer=self.data_lake_layer,
                            target_database=self.target_data_lake_database,
                            source_database=f"db_analytics_{self.env}",
                            target_table_name=self.table_name,
                            data_lake_bucket=self.data_lake_bucket,
                            data_source=self.data_source,
                        )
                    )

                for future in concurrent.futures.as_completed(futures):
                    try:
                        result = future.result()
                    except Exception as e:
                        raise Exception(f"Error while executing the query to write data into the data lakehouse: {e}")

        else:
            write_data_into_datalake_using_ctas(
                sql_query=sql_query,
                filter_value=last_block,
                env=self.env,
                data_lake_layer=self.data_lake_layer,
                target_database=self.target_data_lake_database,
                target_table_name=self.table_name,
                data_lake_bucket=self.data_lake_bucket,
                data_source=self.data_source,
            )

        self.logger.info(
            f"Data written into {self.table_name} table - Data Source: {self.data_source} - Layer: {self.data_lake_layer}"
        )
