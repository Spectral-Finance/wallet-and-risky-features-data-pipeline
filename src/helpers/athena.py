import awswrangler as wr
import pandas as pd
from typing import Iterator

# wr configs
wr.config.max_cache_seconds = 900
wr.config.max_cache_query_inspections = 500
wr.config.max_remote_cache_entries = 50
wr.config.max_local_cache_entries = 100


def _split_query(sql_query: str) -> list:
    """Splits templated sql query into table creation and data insertion.

    Args:
        sql_query (str): Query to be executed

    Returns:
        list: List obj with 2 items containing the table creation and data insertion queries
    """
    return sql_query.split("-- incremental load")


def iterate_over_last_updated_items(
    sql_query: str, last_inserted_timestamp: int, database: str = "db_analytics_prod"
) -> Iterator[pd.DataFrame]:
    """Gets the latest updated items from table.
    Memory efficient way to iterate over the results of a query. No fixed row limit for each chunk.

    Args:
        sql_query (str): Query to be executed
        last_inserted_timestamp (int): Last inserted timestamp
        database (str, optional): Athena database name. Defaults to 'db_analytics_prod'.

    Returns:
        Iterator[pd.DataFrame]: Iterator over the query results
    """

    sql_query = sql_query.replace("last_inserted_timestamp", str(last_inserted_timestamp))

    return wr.athena.read_sql_query(sql=sql_query, database=database, chunksize=True)


def write_data_into_datalake_using_ctas(
    sql_query: str,
    filter_value: str,
    env: str,
    data_lake_layer: str,
    target_database: str,
    target_table_name: str,
    data_lake_bucket,
    data_source,
    source_database: str = None,
) -> None:
    """Function to write data into data lake using CTAS (Create Table As Select)

    Args:
        sql_query (str): Query to be executed
        filter_value (str): Filter value
        env (str): Environment name
        data_lake_layer (str): Layer name
        target_database (str): Target database name
        target_table_name (str): Table name
        data_lake_bucket (str): Data lake bucket name
        data_source (str): Data source name

    Returns:
        None
    """

    sql_query = _split_query(sql_query=sql_query)

    if data_lake_layer == "stage" and source_database is None:
        source_database = f"db_raw_{env}"
    elif data_lake_layer == "analytics" and source_database is None:
        source_database = f"db_stage_{env}"

    if wr.catalog.does_table_exist(database=target_database, table=target_table_name):
        sql_query = sql_query[-1]  # Get the incremental load part
        sql_query = (
            sql_query.replace("filter_value", str(filter_value))
            .replace("source_database", source_database)
            .replace("target_database", target_database)
            .replace("table_name", target_table_name)
            .replace("bucket_name", data_lake_bucket)
            .replace("layer", data_lake_layer)
            .replace("data_source", data_source)
        )

    else:
        sql_query = sql_query[0]  # Get the full load part
        sql_query = (
            sql_query.replace("filter_value", str(filter_value))
            .replace("source_database", source_database)
            .replace("target_database", target_database)
            .replace("table_name", target_table_name)
            .replace("bucket_name", data_lake_bucket)
            .replace("layer", data_lake_layer)
            .replace("data_source", data_source)
        )

    try:
        wr.athena.start_query_execution(sql=sql_query, database=target_database, wait=True)
    except Exception as e:
        raise Exception(f"Error while executing the query to write data into the data lakehouse: {e}")


def write_data_into_datalake_using_ctas_by_chunks(
    sql_query: str,
    chunk: tuple,
    filter_value: str,
    env: str,
    data_lake_layer: str,
    target_database: str,
    target_table_name: str,
    data_lake_bucket,
    data_source,
    source_database: str = None,
) -> None:
    """Write data into data lake using CTAS (Create Table As Select) by chunks

    Args:
        sql_query (str): Query to be executed
        chunk (tuple): Chunk to be processed
        filter_value (str): Filter value
        env (str): Environment name
        data_lake_layer (str): Layer name
        target_database (str): Target database name
        target_table_name (str): Table name
        data_lake_bucket (str): Data lake bucket name
        data_source (str): Data source name

    Returns:
        None
    """

    sql_query = _split_query(sql_query=sql_query)

    if data_lake_layer == "stage" and source_database is None:
        source_database = f"db_raw_{env}"
    elif data_lake_layer == "analytics" and source_database is None:
        source_database = f"db_stage_{env}"

    if wr.catalog.does_table_exist(database=target_database, table=target_table_name):
        sql_query = sql_query[-1]  # Get the incremental load part
        sql_query = (
            sql_query.replace("filter_value", str(filter_value))
            .replace("chunk", str(chunk))
            .replace("source_database", source_database)
            .replace("target_database", target_database)
            .replace("table_name", target_table_name)
            .replace("bucket_name", data_lake_bucket)
            .replace("layer", data_lake_layer)
            .replace("data_source", data_source)
        )

    else:
        sql_query = sql_query[0]  # Get the full load part
        sql_query = (
            sql_query.replace("filter_value", str(filter_value))
            .replace("chunk", str(chunk))
            .replace("source_database", source_database)
            .replace("target_database", target_database)
            .replace("table_name", target_table_name)
            .replace("bucket_name", data_lake_bucket)
            .replace("layer", data_lake_layer)
            .replace("data_source", data_source)
        )

    try:
        wr.athena.start_query_execution(sql=sql_query, database=target_database, wait=True)
    except Exception as e:
        raise Exception(f"Error while executing the query to write data into the data lakehouse: {e}")


def optimize_iceberg_table(target_database: str, table_name: str, chunk: tuple = None) -> None:
    """Optimize iceberg table using bin pack to rewrite data in the table by chunks and avoid small files.

    Args:
        target_database (str): Target database name
        table_name (str): Iceberg table name
        chunk (tuple): Chunk to be processed

    Returns:
        None
    """

    # If chunk is None, it will optimize the entire table
    # If chunk is not None, it will optimize the table by chunks
    query_filter = f"WHERE address_partition in {chunk}" if chunk else ""

    query_optimize = f"""
    OPTIMIZE {target_database}.{table_name} REWRITE DATA USING BIN_PACK {query_filter}
    """

    query_vaccum = f"VACUUM {target_database}.{table_name}"

    try:
        wr.athena.start_query_execution(sql=query_optimize, database=target_database, wait=True)
        wr.athena.start_query_execution(sql=query_vaccum, database=target_database, wait=True)
    except Exception as e:
        raise Exception(f"Error while executing the query to optimize the iceberg table: {e}")
