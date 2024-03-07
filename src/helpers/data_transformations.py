from datetime import datetime
import pandas as pd
import re
import ast

from spectral_data_lib.log_manager import Logger

logger = Logger(logger_name="Data Transformations Helper")


def convert_string_to_dict(cell: str) -> dict:
    """Converts a cell containing a list of tuples into a dictionary.

    This function is designed to transform a cell from a pandas DataFrame column
    where each cell is expected to be a list of tuples. Each tuple should have two elements:
    the first being a key and the second a list of tuples, which will be converted into
    a dictionary.

    Args:
        cell (list of tuples): A cell from a DataFrame, where each element is a tuple.

    Returns:
        dict: A dictionary with the first element of each tuple as key and the second as value.
    """

    return {item[0]: dict(item[1]) for item in cell}


def add_partition_column(data: pd.DataFrame, column: str = None) -> pd.DataFrame:
    """Function to add partition columns to the dataframe.

    Args:
        data (pd.DataFrame): Dataframe to add partition columns.
        column (str): Column to add partition columns.

    Returns:
        pd.DataFrame: Dataframe
    """

    if column is None:

        logger.info("Adding partition column to dataframe.")

        data["date_partition"] = datetime.now().strftime("%Y-%m")

        logger.info("Partition column added to dataframe - Partition column: timestamp")

    else:

        logger.info("Adding partition column to dataframe.")

        data["date_partition"] = pd.to_datetime(data[column], unit="s").dt.strftime("%Y-%m")

        logger.info(f"Partition column added to dataframe - Partition column: {column}")

    return data


def convert_timestamp_to_datetime(data: pd.DataFrame, column: str) -> pd.DataFrame:
    """Function to convert timestamp to datetime.

    Args:
        data (pd.DataFrame): Dataframe to convert timestamp to datetime.
        column (str): Column to convert timestamp to datetime.

    Returns:
        pd.DataFrame: Dataframe
    """

    logger.info("Converting timestamp to datetime.")

    data[column] = pd.to_datetime(data[column], unit="s")

    logger.info(f"Timestamp converted to datetime - Column: {column}")

    return data
