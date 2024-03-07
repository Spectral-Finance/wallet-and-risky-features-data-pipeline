import awswrangler as wr

from config import settings
from spectral_data_lib.log_manager import Logger
from spectral_data_lib.config import settings as sdl_settings
from spectral_data_lib.data_lakehouse import DataLakehouse
from src.helpers.files import read_sql_file
from src.helpers.athena import write_data_into_datalake_using_ctas

# from src.schemas.stage_layer import ETHEREUM_TABLES_SCHEMA


class StagePipeline(object):
    """Class to create a stage pipeline"""

    def __init__(self, table_name: str) -> None:
        """Constructor for the class

        Args:
            table_name (str): Table name

        Returns:
            None
        """
        self.logger = Logger(logger_name=f"Ethereum - Stage Pipeline Logger")
        self.data_lake_layer = "stage"
        self.table_name = table_name
        self.target_data_lake_database = sdl_settings.DATA_LAKE_STAGE_DATABASE
        self.data_lake_bucket = sdl_settings.DATA_LAKE_BUCKET_S3
        self.data_source = "ethereum"
        self.env = settings.ENV
        self.data_lakehouse_connection = DataLakehouse()

    def get_last_row_from_table(self) -> int:
        """Function to get the last block from the table in the data lakehouse

        Args:
            None

        Returns:
            int: Last timestamp
        """

        table_exist = wr.catalog.does_table_exist(database=self.target_data_lake_database, table=self.table_name)

        if table_exist:

            self.logger.info(f"Table {self.table_name} exist - Incremental ingestion.")

            query_to_get_last_row_inserted = f"""
            SELECT MAX(column) AS last_row_inserted
            FROM {self.target_data_lake_database}.{self.table_name}
            WHERE date_partition in (
                    SELECT MAX(date_partition) AS last_partition
                    FROM {self.target_data_lake_database}.{self.table_name}
                )
            """

            if self.table_name == "ethereum_blocks":
                last_row_inserted = self.data_lakehouse_connection.read_sql_query(
                    query=query_to_get_last_row_inserted.replace("column", "number"),
                    database_name=self.target_data_lake_database,
                )["last_row_inserted"][0]

            elif self.table_name == "ethereum_contracts":
                last_row_inserted = self.data_lakehouse_connection.read_sql_query(
                    query=query_to_get_last_row_inserted.replace("column", "block_timestamp"),
                    database_name=self.target_data_lake_database,
                )["last_row_inserted"][0]

            elif self.table_name == "ethereum_tokens":
                last_row_inserted = self.data_lakehouse_connection.read_sql_query(
                    query=query_to_get_last_row_inserted.replace("column", "block_timestamp"),
                    database_name=self.target_data_lake_database,
                )["last_row_inserted"][0]

            elif (
                self.table_name == "ethereum_tokens_metadata"
            ):  # For tokens metadata table we use the created_timestamp column as last_block (last_created_at)
                last_row_inserted = self.data_lakehouse_connection.read_sql_query(
                    query=query_to_get_last_row_inserted.replace("column", "created_timestamp"),
                    database_name=self.target_data_lake_database,
                )["last_row_inserted"][0]

            else:
                last_row_inserted = self.data_lakehouse_connection.read_sql_query(
                    query=query_to_get_last_row_inserted.replace("column", "block_number"),
                    database_name=self.target_data_lake_database,
                )["last_row_inserted"][0]

        else:  # Default values for the first time

            self.logger.info(f"Table {self.table_name} does not exist - Full ingestion.")

            if self.table_name == "ethereum_transactions":
                last_row_inserted = 46147
            elif self.table_name == "ethereum_logs":
                last_row_inserted = 52029
            elif self.table_name == "ethereum_token_transfers":
                last_row_inserted = 447767
            elif (
                self.table_name == "ethereum_contracts"
                or self.table_name == "ethereum_tokens"
                or self.table_name == "ethereum_tokens_metadata"
            ):
                last_row_inserted = "2015-01-01 00:00:00.000"
            else:
                last_row_inserted = 0

        return last_row_inserted

    def run(self, **kwargs) -> None:
        """Function to run the pipeline

        Args:
            **kwargs: Keyword arguments
        """
        self.logger.info("Stage pipeline started")

        sql_file_path = f"src/pipelines/{self.data_lake_layer}/transformations/{self.table_name}.sql"

        self.logger.info(
            f"Running data ingestion - Data Source: {self.data_source} - Table: {self.table_name} - Layer: {self.data_lake_layer}"
        )

        sql_query = read_sql_file(file_path=sql_file_path)

        last_row_inserted = self.get_last_row_from_table()

        self.logger.info(
            f"Last row inserted (block_timestamp or block_number): {last_row_inserted} - Data Source: {self.data_source} - Table: {self.table_name} - Layer: {self.data_lake_layer}"
        )

        write_data_into_datalake_using_ctas(
            sql_query=sql_query,
            filter_value=last_row_inserted,
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
