import awswrangler as wr
import numpy as np
from itertools import product
from concurrent.futures import ThreadPoolExecutor
from multiprocessing import cpu_count
from datetime import datetime

from config import settings
from spectral_data_lib.log_manager import Logger
from spectral_data_lib.config import settings as sdl_settings
from src.helpers.files import read_sql_file
from src.helpers.athena import (
    write_data_into_datalake_using_ctas,
    write_data_into_datalake_using_ctas_by_chunks,
    iterate_over_last_updated_items,
    optimize_iceberg_table,
)
from src.helpers.data_transformations import convert_string_to_dict

from spectral_data_lib.feature_data_documentdb.sync_mongo_connection import SyncMongoConnection
from spectral_data_lib.data_lakehouse import DataLakehouse


class FeaturesPipeline(object):
    """Class to create a features pipeline"""

    def __init__(self, table_name: str) -> None:
        sdl_settings.SECRET_NAME = settings.FEATURE_DB_SECRET_NAME
        self.logger = Logger(logger_name=f"Ethereum - Features Pipeline Logger")
        self.data_lake_layer = "analytics"
        self.data_lake_bucket = sdl_settings.DATA_LAKE_BUCKET_S3
        self.target_data_lake_database = sdl_settings.DATA_LAKE_ANALYTICS_DATABASE
        self.table_name = table_name
        self.data_source = "ethereum"
        self.env = settings.ENV
        self.update_features_db_query_dir = settings.UPDATED_FEATURES_DB_QUERY_DIR
        self.features_db_connection = SyncMongoConnection(
            addition_connection_parameters_string=settings.MONGO_RETRY_WRITE_TO_FALSE
        )
        self.data_lakehouse_connection = DataLakehouse()

    def get_last_timestamp_inserted(self, filter: str = None) -> int:
        """Function to get the last timestamp inserted in the data lakehouse

        Args:
            None

        Returns:
            int: Last timestamp
        """

        table_exist = wr.catalog.does_table_exist(database=self.target_data_lake_database, table=self.table_name)

        if table_exist:

            self.logger.info(f"Table {self.table_name} exist - Incremental ingestion.")

            query_to_get_last_timestamp_inserted = f"""
            SELECT MAX(column) AS last_timestamp_inserted
            FROM {self.target_data_lake_database}.{self.table_name} {filter if filter else ""}
            """

            timestamp_column = (
                "last_interaction_timestamp" if self.table_name == "rugpull_features" else "wallet_last_tx"
            )

            last_timestamp_inserted = self.data_lakehouse_connection.read_sql_query(
                query=query_to_get_last_timestamp_inserted.replace("column", timestamp_column),
                database_name=self.target_data_lake_database,
            )["last_timestamp_inserted"][0]

        else:  # Default values for the first time
            self.logger.info(f"Table {self.table_name} does not exist - Full ingestion.")
            last_timestamp_inserted = 0

        return last_timestamp_inserted

    def run(self, **kwargs) -> None:
        """Implements the `Features` pipeline

        Args:
            **kwargs: Keyword arguments
        """

        self.logger.info(f"Features pipeline started for {self.table_name}")

        sql_file_path = f"src/pipelines/{self.data_lake_layer}/features/queries/{self.table_name}.sql"

        self.logger.info(
            f"Running data processing - Data Source: {self.data_source} - Table: {self.table_name} - Layer: {self.data_lake_layer}"
        )

        sql_query = read_sql_file(file_path=sql_file_path)

        if self.table_name == "ethereum_wallet_features":

            addresses_partitions = list(
                map("".join, product("0123456789abcdef", repeat=2))
            )  # generate all the possible addresses partitions (256)
            addresses_partitions_chunks = np.array_split(addresses_partitions, 20)

            # We are using a CTAS to write the data into the data lakehouse by chunks.
            # We are using iceberg tables, which are not compatible with concurrent writes at this moment.
            for addresses_partition_chunk in addresses_partitions_chunks:

                addresses_partition_chunk = tuple(addresses_partition_chunk)

                # Athena query to get the last timestamp inserted in the data lakehouse by chunk.
                # We are doing this to avoid writing the same data multiple times in the data lakehouse.
                # Because if we use a single query to get the last timestamp inserted, we can have issues if a new data is inserted while we are writing the data.
                last_timestamp_insert = self.get_last_timestamp_inserted(
                    filter=f"WHERE address_partition IN {addresses_partition_chunk}"
                )

                write_data_into_datalake_using_ctas_by_chunks(
                    sql_query=sql_query,
                    chunk=addresses_partition_chunk,
                    filter_value=last_timestamp_insert,
                    env=self.env,
                    data_lake_layer=self.data_lake_layer,
                    target_database=self.target_data_lake_database,
                    source_database=f"db_analytics_{self.env}",
                    target_table_name=self.table_name,
                    data_lake_bucket=self.data_lake_bucket,
                    data_source=self.data_source,
                )

                if datetime.today().weekday() == 6:  # Sunday

                    self.logger.info(
                        f"Optimizing Iceberg table for addresses partitions: {addresses_partition_chunk} to avoid small files and improve performance."
                    )
                    optimize_iceberg_table(
                        target_database=self.target_data_lake_database,
                        table_name=self.table_name,
                        chunk=addresses_partition_chunk,
                    )
                    self.logger.info("Iceberg table optimized.")

                self.logger.info(
                    f"Data written into data lakehouse for addresses partitions: {addresses_partition_chunk}"
                )

        else:
            # Updates datalake house data
            write_data_into_datalake_using_ctas(
                sql_query=sql_query,
                filter_value=None,
                env=self.env,
                data_lake_layer=self.data_lake_layer,
                target_database=self.target_data_lake_database,
                source_database=f"db_analytics_{self.env}",
                target_table_name=self.table_name,
                data_lake_bucket=self.data_lake_bucket,
                data_source=self.data_source,
            )

            if datetime.today().weekday() == 6:  # Sunday

                self.logger.info(f"Optimizing Iceberg table to avoid small files and improve performance.")
                optimize_iceberg_table(target_database=self.target_data_lake_database, table_name=self.table_name)
                self.logger.info("Iceberg table optimized.")

        # Updates features db data
        sql_query = read_sql_file(
            file_path=f"{self.update_features_db_query_dir}/{self.table_name}_data_to_features_db.sql"
        )

        # Athena query to get the last timestamp inserted in the data lakehouse after the data ingestion
        last_timestamp_inserted_data_lakehouse = self.get_last_timestamp_inserted()

        # MongoDB aggregation pipeline to get the last timestamp inserted in the features db
        if self.table_name == "rugpull_features":

            features_db_query = {"collectionName": self.table_name}
            field = "last_interaction_timestamp"

        else:  # wallet_features
            features_db_query = {
                "collectionName": self.table_name.replace("ethereum_", "")
                if self.table_name.startswith("ethereum_")
                else self.table_name
            }
            field = "wallet_last_tx"

        last_timestamp_inserted_features_db = self.features_db_connection.get_data(
            db_name="features_db",
            collection_name="collections_metadata",
            filter=features_db_query,
            attributes_to_project=[field],
        )

        last_timestamp_inserted_features_db = (
            last_timestamp_inserted_features_db[0][field] if last_timestamp_inserted_features_db else 0
        )

        # If there is new data in the data lakehouse, update the features db
        # Comparing the last timestamp before and after the data ingestion to get only new data inserted to insert into the features db
        if last_timestamp_inserted_data_lakehouse > last_timestamp_inserted_features_db:

            new_last_timestamp_inserted_features_db = 0

            for batch in iterate_over_last_updated_items(
                sql_query=sql_query, last_inserted_timestamp=last_timestamp_inserted_features_db
            ):  # pagination

                new_data = batch.copy()
                new_data.rename(columns={"wallet_address": "walletAddress"}, inplace=True)

                if self.table_name == "ethereum_wallet_features":

                    new_data["contracts_aggregations"] = new_data["contracts_aggregations"].apply(
                        convert_string_to_dict
                    )
                    new_data.rename(columns={"contracts_aggregations": "contracts"}, inplace=True)

                if new_last_timestamp_inserted_features_db < new_data[field].max():
                    new_last_timestamp_inserted_features_db = new_data[field].max()

                chunks = np.array_split(new_data, cpu_count())

                with ThreadPoolExecutor(max_workers=cpu_count()) as executor:
                    executor.map(
                        self.features_db_connection.update_documents,
                        [
                            {
                                "db_name": "features_db",
                                "collection_name": self.table_name.replace("ethereum_", "")
                                if self.table_name.startswith("ethereum_")
                                else self.table_name,
                                "key_to_match": "walletAddress",
                                "update_collection": chunk.to_dict("records"),
                                "upsert": True,
                            }
                            for chunk in chunks
                        ],
                    )

            # Update the last timestamp inserted in the features db after the data ingestion in the collection metadata
            # This collection metadata is used to get the last timestamp inserted in the features db instead execute the MongoDB aggregation pipeline every time
            collections_metadata = [
                {
                    "collectionName": self.table_name.replace("ethereum_", "")
                    if self.table_name.startswith("ethereum_")
                    else self.table_name,
                    field: int(new_last_timestamp_inserted_features_db),
                },
            ]

            self.features_db_connection.update_documents(
                db_name="features_db",
                collection_name="collections_metadata",
                key_to_match="collectionName",
                update_collection=collections_metadata,
                upsert=True,
            )

            self.logger.info(
                f"Data written into {self.table_name} table - Data Source: {self.data_source} - Layer: {self.data_lake_layer}"
            )

        else:
            self.logger.info(
                f"Data already updated in features db - Data Source: {self.data_source} - Layer: {self.data_lake_layer}"
            )
