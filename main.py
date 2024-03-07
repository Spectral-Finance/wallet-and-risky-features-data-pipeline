import importlib
from argparse import ArgumentParser
from spectral_data_lib.log_manager import Logger

logger = Logger(logger_name=f"Ethereum - Wallets Transactions Data Pipeline Logger")


def main():
    """Main function to run the pipeline."""

    parser = ArgumentParser(description="This script is used to the Ethereum Wallets Transactions Data Pipeline.")
    parser.add_argument("--start-block", type=int, required=False)
    parser.add_argument("--end-block", type=int, required=False)
    parser.add_argument("--table-name", type=str, help="Table Name", required=False)
    parser.add_argument("--data-lake-layer", type=str, help="Data Lake Layer", required=True)

    args = parser.parse_args()

    if args.data_lake_layer == "raw":

        # This module will be imported just in case the data lake layer is raw, otherwise it will not be imported.
        # This is necessary because this main.py file is used to run all the pipelines, and when we are using this module in EMR Serverless (Spark),
        # it will try to import all the modules, even if they are not necessary.
        raw_pipeline_module = importlib.import_module(f"src.pipelines.raw.raw_data_ingestion_pipeline")

        raw_pipeline = raw_pipeline_module.RawPipeline()

        last_block_data_lakehouse = args.start_block
        last_block_ethereum_node = args.end_block

        raw_pipeline.run(
            last_block_data_lakehouse=last_block_data_lakehouse,
            last_block_ethereum_node=last_block_ethereum_node,
        )

    elif args.data_lake_layer == "stage":

        stage_pipeline_module = importlib.import_module(f"src.pipelines.stage.stage_data_ingestion_pipeline")

        stage_pipeline = stage_pipeline_module.StagePipeline(table_name=args.table_name)

        stage_pipeline.run()

    elif args.data_lake_layer == "analytics":

        analytics_pipeline_module = importlib.import_module("src.pipelines.analytics.analytics_data_ingestion_pipeline")

        analytics_pipeline = analytics_pipeline_module.AnalyticsPipeline(table_name=args.table_name)

        analytics_pipeline.run()

    elif args.data_lake_layer == "features":
        features_module = importlib.import_module("src.pipelines.analytics.features.features_pipeline")

        features_pipeline = features_module.FeaturesPipeline(table_name=args.table_name)

        features_pipeline.run()

    elif args.data_lake_layer == "features_data_quality":

        features_data_quality_module = importlib.import_module(
            "src.pipelines.analytics.features.features_data_quality_pipeline"
        )

        features_data_quality_pipeline = features_data_quality_module.FeatureDataQualityPipeline(
            table_name=args.table_name
        )

        features_data_quality_pipeline.run()


if __name__ == "__main__":

    main()
