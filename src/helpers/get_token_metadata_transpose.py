import pandas as pd
import requests
import time
from config import settings
from spectral_data_lib.log_manager import Logger
from spectral_data_lib.data_lakehouse import DataLakehouse


class TranposeTokenMetadata:
    """Class to load token metadata from Transpose API using SQL query API."""

    def __init__(self, api_key: str, last_timestamp_inserted: str = "2015-07-30 00:00:00.000"):
        self.api_key = api_key
        self.last_timestamp_inserted = last_timestamp_inserted
        self.logger = Logger(logger_name="load_historical_token_metadata_transpose")
        self.data_lakehouse_client = DataLakehouse()
        self.headers = {
            "Content-Type": "application/json",
            "X-API-KEY": self.api_key,
        }

    def get_token_metadata(self, limit: int, offset: int, retries: int = 2) -> dict:
        """Get token metadata from Transpose API using SQL query API.

        Args:
            limit (int): Number of rows to fetch.
            offset (int): Offset to fetch rows.
            retries (int): Number of retries.

        Returns:
            dict: Data from Transpose API.

        """

        parameters = {
            "last_timestamp_inserted": self.last_timestamp_inserted,
            "limit_param": str(limit),
            "offset_param": str(offset),
        }

        try:
            response = requests.get(
                url=settings.TRANSPOSE_API_TOKENS_METADATA_ENDPOINT, headers=self.headers, params=parameters
            )
            if response.status_code == 200:
                return response.json()
            else:
                if retries > 0:
                    self.logger.error(f"Failed to fetch data: {response.status_code} - {response.text}")
                    self.logger.info(f"Retrying to fetch data - Retries left: {retries}")
                    time.sleep(1)
                    return self.get_token_metadata(limit=limit, offset=offset, retries=retries - 1)
                else:
                    self.logger.error(f"Failed to fetch data: {response.status_code} - {response.text}")
                    return None

        except Exception as e:
            if retries > 0:
                self.logger.error(f"Failed to fetch data: {e}")
                self.logger.info(f"Retrying to fetch data - Retries left: {retries}")
                time.sleep(1)
                return self.get_token_metadata(limit=limit, offset=offset, retries=retries - 1)
            else:
                self.logger.error(f"Failed to fetch data: {e}")
                return None

    @staticmethod
    def convert_timestamp_to_datetime(data: pd.DataFrame, column: str) -> pd.DataFrame:
        """Convert timestamp to datetime.

        Args:
            data (pd.DataFrame): Dataframe.
            column (str): Column to convert.

        Returns:
            pd.DataFrame: Dataframe with column converted to datetime.
        """

        data[column] = pd.to_datetime(data[column])
        return data

    @staticmethod
    def fill_missing_last_refreshed(data: pd.DataFrame) -> pd.DataFrame:
        """Fill missing last refreshed with created timestamp.

        Args:
            data (pd.DataFrame): Dataframe.

        Returns:
            pd.DataFrame: Dataframe with last refreshed filled.
        """

        data["last_refreshed"] = data["last_refreshed"].fillna(data["created_timestamp"])
        return data

    def run(self):
        """Run the pipeline.

        Args:
            None

        Returns:
            None
        """

        limit = 50000
        offset = 0
        all_data = []

        while True:
            data = self.get_token_metadata(limit, offset)
            if data is None or not data["results"]:
                break
            all_data.extend(data["results"])
            offset += limit
            time.sleep(1)  # To avoid hitting any rate limits

        all_data = pd.DataFrame(all_data)

        if all_data.empty:
            self.logger.info("No data to load.")
            return None
        else:
            all_data = self.convert_timestamp_to_datetime(data=all_data, column="created_timestamp")
            all_data = self.convert_timestamp_to_datetime(data=all_data, column="last_refreshed")
            all_data = self.fill_missing_last_refreshed(data=all_data)
            return all_data


# if __name__ == "__main__":
#     api_key = get_secret("prod/transpose_api_key")["api_key"]
#     loader = TranposeTokenMetadata(api_key=api_key)
#     loader.run()
