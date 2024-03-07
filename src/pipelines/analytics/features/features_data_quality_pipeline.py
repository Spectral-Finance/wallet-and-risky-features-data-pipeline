from config import settings
from spectral_data_lib.log_manager import Logger
from spectral_data_lib.config import settings as sdl_settings
from src.helpers.files import read_sql_file

from spectral_data_lib.data_lakehouse import DataLakehouse


class FeatureDataQualityPipeline(object):
    def __init__(self, table_name: str) -> None:
        self.logger = Logger(logger_name=f"Features - Data Quality Pipeline Logger")
        self.table_name = table_name
        self.target_data_lake_database = sdl_settings.DATA_LAKE_ANALYTICS_DATABASE
        self.env = settings.ENV
        self.data_lakehouse_connection = DataLakehouse()

    def check_data_quality(self, sql_query_path: str) -> None:
        """Check data quality for a feature set based on a SQL query.

        Args:
            sql_query_path (str): Path to the SQL query to be executed.

        Returns:
            None
        """

        sql_query = read_sql_file(sql_query_path)
        data_quality_df_checks = self.data_lakehouse_connection.read_sql_query(
            query=sql_query,
            database_name=self.target_data_lake_database,
        )

        fail_data_quality_df_checks = data_quality_df_checks[data_quality_df_checks["is_fail"] == True][
            "constraint_name"
        ]

        if fail_data_quality_df_checks.shape[0] == 0:
            self.logger.info("No data quality issues found.")
        else:
            raise Exception(
                f"Data quality issues found: \n{fail_data_quality_df_checks.to_markdown(headers='keys', index='check_name')}"
            )

    def run(self) -> None:
        """Run data quality checks for a feature set.

        Args:
            None

        Returns:
            None
        """

        self.logger.info(f"Executing data quality checks for feature set {self.table_name}")

        sql_query_path = f"src/pipelines/analytics/features/queries/data_quality_{self.table_name}.sql"

        self.check_data_quality(sql_query_path=sql_query_path)
