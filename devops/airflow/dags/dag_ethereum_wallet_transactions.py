import os
from datetime import datetime, timedelta

import numpy as np
from airflow.models import Variable, DagRun
from airflow.models.dag import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.operators.python_operator import BranchPythonOperator
from airflow.utils.task_group import TaskGroup
from airflow.providers.amazon.aws.operators.ecs import ECSOperator

from helper_dag import ecs_task_template, slack_alert
from wallet_transactions_utils import get_last_block_data_lakehouse, get_last_block_from_ethereum_blockchain

ARGS = {
    "owner": "Spectral",
    "description": "Dag to ingest data from Ethereum Node (Wallet Transactions) into Data Lakehouse",
    "retry": 3,
    "retry_delay": timedelta(minutes=1),
    "start_date": datetime(2023, 1, 10),
    "depend_on_past": False,
    "on_failure_callback": slack_alert,
}

DAG_ID = os.path.basename(__file__).replace(".py", "")
ENV = Variable.get("environment")
PROJECT_NAME = "wallet-and-risky-features-data-pipeline"
MEMORY_RESERVATION = 8192
STACK_NAME = PROJECT_NAME


def get_range_of_blocks(**kwargs):
    """Function to get range of blocks to be processed.

    Args:
        None

    Returns:
        start_block (int): Start block to be processed.
        end_block (int): End block to be processed.
    """

    dag_runs = DagRun.find(dag_id=DAG_ID)
    start_block = Variable.get("ethereum_start_block", 0)
    end_block = Variable.get("ethereum_end_block", 0)

    if start_block == 0 and end_block == 0:

        start_block = get_last_block_data_lakehouse(data_lake_database=f"db_raw_{ENV}")
        end_block = get_last_block_from_ethereum_blockchain()

        Variable.set("ethereum_start_block", start_block)
        Variable.set("ethereum_end_block", end_block)

        return start_block, end_block

    else:
        if dag_runs:
            for dag_run in dag_runs:
                if dag_run.state == "running" or start_block != 0 or end_block != 0:
                    return int(start_block), int(end_block)

                else:

                    start_block = get_last_block_data_lakehouse(data_lake_database=f"db_raw_{ENV}")
                    end_block = get_last_block_from_ethereum_blockchain()

                    Variable.set("ethereum_start_block", start_block)
                    Variable.set("ethereum_end_block", end_block)

                    return start_block, end_block
        else:
            return int(start_block), int(end_block)


def check_if_there_are_blocks_to_fetch(**kwargs):
    """Function to check if the data pipeline should be executed.

    Args:
        None

    Returns:
        None
    """

    start_block = Variable.get("ethereum_start_block", 0)
    end_block = Variable.get("ethereum_end_block", 0)

    if int(end_block) - int(start_block) > 0:
        return "execute_data_ingestion_pipeline"
    else:
        return "skip_data_ingestion_send_slack_alert_and_delete_blocks_variables"


def send_slack_alert_and_delete_blocks_variables(**kwargs):
    """Function to send slack alert and delete blocks variables.
    This is necessary to avoid the DAG to be executed again with the same blocks range.

    Args:
        None

    Returns:
        None
    """

    Variable.delete("ethereum_start_block")
    Variable.delete("ethereum_end_block")

    kwargs["exception"] = "There are no blocks to fetch. The DAG will be skipped."

    slack_alert(kwargs)


def send_slack_alert_data_quality(kwargs):
    """Function to send slack alert and delete blocks variables.
    This is necessary to avoid the DAG to be executed again with the same blocks range.

    Args:
        None

    Returns:
        None
    """

    kwargs["alert_type"] = "data_quality"

    slack_alert(kwargs)


def delete_blocks_variables(**kwargs):
    """Function to delete blocks variables.
    This is necessary to avoid the DAG to be executed again with the same blocks range.

    Args:
        None

    Returns:
        None
    """

    Variable.delete("ethereum_start_block")
    Variable.delete("ethereum_end_block")


with DAG(
    dag_id=DAG_ID,
    default_args=ARGS,
    schedule_interval="0 */5 * * *",  # "@hourly",
    tags=["DATA_LAKEHOUSE", "ETHEREUM_NODE", "WALLET_TRANSACTIONS", "WALLET_EVENTS"],
    catchup=False,
    max_active_runs=1,
) as dag:

    task_start = DummyOperator(task_id="start")
    task_teardown = DummyOperator(task_id="teardown")

    task_check_if_there_are_blocks_to_fetch = BranchPythonOperator(
        task_id="check_if_there_are_blocks_to_fetch",
        python_callable=check_if_there_are_blocks_to_fetch,
        dag=dag,
    )

    task_skip_data_ingestion_pipeline = PythonOperator(
        task_id="skip_data_ingestion_send_slack_alert_and_delete_blocks_variables",
        python_callable=send_slack_alert_and_delete_blocks_variables,
        dag=dag,
    )

    start_block, end_block = get_range_of_blocks()

    if end_block - start_block > 0:

        task_execute_data_ingestion_pipeline = DummyOperator(task_id="execute_data_ingestion_pipeline")

        task_delete_blocks_variables = PythonOperator(
            task_id="delete_blocks_variables", python_callable=delete_blocks_variables, dag=dag
        )

        if start_block + 5000 < end_block:

            end_block = start_block + 5000

        blocks_range = np.arange(start_block + 1, end_block + 1)

        blocks_chunks = np.array_split(blocks_range, 5) if len(blocks_range) >= 1000 else [blocks_range]

        with TaskGroup(group_id="Raw_Layer") as raw_layer_tg:

            for chunk in blocks_chunks:

                start_block = str(chunk[0])
                end_block = str(chunk[-1])

                task_ecs_raw = ECSOperator(
                    task_id=f"fetch_and_save_wallet_transactions_raw_data_from_block_{start_block}_to_block_{end_block}",
                    execution_timeout=timedelta(minutes=60),
                    **ecs_task_template(
                        command_list=[
                            "python",
                            "main.py",
                            "--start-block",
                            start_block,
                            "--end-block",
                            end_block,
                            "--data-lake-layer",
                            "raw",
                        ],
                        stack_name=f"{PROJECT_NAME}-{ENV}",
                        project=PROJECT_NAME,
                        stream_log_prefix=PROJECT_NAME,
                        memory_reservation=MEMORY_RESERVATION,
                    ),
                )

        stage_tables = [
            "ethereum_logs",
            "ethereum_transactions",
            "ethereum_blocks",
            "ethereum_token_transfers",
            "ethereum_traces",
            "ethereum_contracts",
            "ethereum_tokens",
            "ethereum_tokens_metadata",
        ]

        task_start_stage_layer = DummyOperator(task_id="start_stage_layer")

        with TaskGroup(group_id="Stage_Layer") as stage_layer_tg:

            for table in stage_tables:

                task_ecs_stage = ECSOperator(
                    task_id=f"apply_transformation_and_save_stage_data_{table}",
                    execution_timeout=timedelta(minutes=60),
                    **ecs_task_template(
                        command_list=["python", "main.py", "--table-name", table, "--data-lake-layer", "stage"],
                        stack_name=f"{PROJECT_NAME}-{ENV}",
                        project=PROJECT_NAME,
                        stream_log_prefix=PROJECT_NAME,
                        memory_reservation=MEMORY_RESERVATION,
                    ),
                )

        analytics_tables = [
            "ethereum_erc20_transactions",
            "ethereum_normal_transactions",
            "ethereum_internal_transactions",
        ]

        task_start_analytics_layer = DummyOperator(task_id="start_analytics_layer")

        with TaskGroup(group_id="Analytics_Layer") as analytics_layer_tg:

            for table in analytics_tables:

                task_ecs_analytics = ECSOperator(
                    task_id=f"apply_transformation_and_save_analytics_data_{table}",
                    execution_timeout=timedelta(minutes=60),
                    **ecs_task_template(
                        command_list=["python", "main.py", "--table-name", table, "--data-lake-layer", "analytics"],
                        stack_name=f"{PROJECT_NAME}-{ENV}",
                        project=PROJECT_NAME,
                        stream_log_prefix=PROJECT_NAME,
                        memory_reservation=MEMORY_RESERVATION,
                    ),
                )

        # Task to merge erc20, normal and internal transactions to create wallet transactions table
        task_ecs_wallet_transactions = ECSOperator(
            task_id="apply_transformation_and_save_analytics_data_ethereum_wallet_transactions",
            execution_timeout=timedelta(minutes=60),
            **ecs_task_template(
                command_list=[
                    "python",
                    "main.py",
                    "--table-name",
                    "ethereum_wallet_transactions",
                    "--data-lake-layer",
                    "analytics",
                ],
                stack_name=f"{PROJECT_NAME}-{ENV}",
                project=PROJECT_NAME,
                stream_log_prefix=PROJECT_NAME,
                memory_reservation=MEMORY_RESERVATION,
            ),
        )

        features_tables = ["rugpull_features", "ethereum_wallet_features"]

        with TaskGroup(group_id="Features_Layer") as features_layer_tg:

            for table in features_tables:

                task_ecs_features = ECSOperator(
                    task_id=f"calculate_and_save_{table}",
                    execution_timeout=timedelta(minutes=60),
                    **ecs_task_template(
                        command_list=["python", "main.py", "--table-name", table, "--data-lake-layer", "features"],
                        stack_name=f"{PROJECT_NAME}-{ENV}",
                        project=PROJECT_NAME,
                        stream_log_prefix=PROJECT_NAME,
                        memory_reservation=MEMORY_RESERVATION,
                    ),
                )

        task_wallet_features_dq = ECSOperator(
            task_id="wallet_features_data_quality",
            execution_timeout=timedelta(minutes=60),
            **ecs_task_template(
                command_list=[
                    "python",
                    "main.py",
                    "--table-name",
                    "ethereum_wallet_features",
                    "--data-lake-layer",
                    "features_data_quality",
                ],
                stack_name=f"{PROJECT_NAME}-{ENV}",
                project=PROJECT_NAME,
                stream_log_prefix=PROJECT_NAME,
                memory_reservation=MEMORY_RESERVATION,
            ),
            on_failure_callback=send_slack_alert_data_quality,
        )

        (
            task_start
            >> task_check_if_there_are_blocks_to_fetch
            >> task_execute_data_ingestion_pipeline
            >> raw_layer_tg
            >> task_start_stage_layer
            >> stage_layer_tg
            >> task_start_analytics_layer
            >> analytics_layer_tg
            >> task_ecs_wallet_transactions
            >> features_layer_tg
            >> task_wallet_features_dq
            >> task_delete_blocks_variables
            >> task_teardown
        )

    task_start >> task_check_if_there_are_blocks_to_fetch >> task_skip_data_ingestion_pipeline >> task_teardown
