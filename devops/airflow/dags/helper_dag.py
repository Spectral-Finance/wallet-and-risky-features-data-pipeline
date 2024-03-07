from typing import List, Dict, Any
from airflow.models import Variable
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator


def ecs_task_template(
    command_list: List[str],
    stack_name: str,
    project: str,
    memory_reservation: int,
    stream_log_prefix: str,
    container: str = "main-container",
) -> Dict[str, Any]:
    """ECS Task Template.

    Airflow Variables. You must configure an Airflow Variable called "PRICE_AGGREGATOR_AWS_CONFIG"
    using the following pattern for the value field.
      {"env": "dev", "region": "xx-xxx-0", "subnets": ["subnet-0x000"], "security_groups": ["sg-0x000"]}

    For more ECS Task configuration options, check the following link.
      https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/ecs.html#ECS.Client.run_task

    Args:
        command_list (List[str]): Command list to ECR image.

                             So, you must pass the complete of the commands, e.g. ['python',
                                          'src/preparation/database_operations.py',
                                          '--db',
                                          TEMP_DB_NAME,
                                          '--db_operation',
                                          'create_temp_db']
        stack_name (str): Cloud Formation Stack Name (without environment prefix).
                           e.g. AWS Stack -> 'dev-subgraph', here you should pass 'dev-subgraph' only.
        container (str): Task Definition Container Name.
        project (str): Project Description
        memory_reservation (int): Fargate Memory Soft Limit (Max Value: 16384).
        stream_log_prefix (str): StreamLog prefix created with AWS cloudFormation.

    Returns:
        ECS Task configuration dictionary.
    """
    aws_config = Variable.get("PRICE_AGGREGATOR_AWS_CONFIG", deserialize_json=True)

    env = (
        Variable.get("environment")
        if stack_name
        in ["data-ingestion-subgraph-prod", "wallet-and-risky-features-data-pipeline-prod", "defi-features-elt-prod"]
        or project in ["historical-daily-prices", "defi_state", "daily_state", "new-top-coins", "rugpull"]
        else aws_config.get("env", "dev")
    )

    stack = stack_name
    return {
        "aws_conn_id": "aws_default",
        "region_name": aws_config.get("region"),
        "launch_type": "FARGATE",
        "cluster": f"{stack}-fargate-cluster",
        "task_definition": f"{stack}-fargate",
        "network_configuration": {
            "awsvpcConfiguration": {
                "assignPublicIp": "ENABLED",
                "subnets": aws_config.get("subnets"),
                "securityGroups": aws_config.get("security_groups"),
            }
        },
        "awslogs_group": f"/ecs/log-group-{stack}",
        "awslogs_stream_prefix": f"{stream_log_prefix}/{container}",
        "overrides": {
            "containerOverrides": [
                {
                    "name": container,
                    "memoryReservation": memory_reservation,
                    "command": command_list,
                    "environment": [{"name": "APP_ENV", "value": env}],
                }
            ]
        },
        "tags": {"team": "Ml1 - Spectral", "project": project, "environment": env},
    }


def slack_alert(context):
    """
    This function is called when a Task execution failed.
    """

    # Get task data from context
    task = context.get("task_instance")
    alert_type = context.get("alert_type", None)
    dag_name = task.dag_id
    task_name = task.task_id
    error_message = context.get("exception") or context.get("reason")
    execution_date = context.get("execution_date")

    airflow_url = Variable.get("AIRFLOW_URL")
    dag_url = f"{airflow_url}/graph?dag_id={dag_name}"

    if alert_type == "data_quality":

        slack_conn_id = "slack_webhook_data_quality"
        message = f"""
        Data Quality Alert - :airflow_spin: Airflow DAG: *{dag_name}* | Feature Set: *{task_name}* :fail:
        *Execution date*: {execution_date}
        *Error message*: {error_message}
        *DAG URL*: {dag_url}
        """

    else:
        slack_conn_id = "slack_webhook"
        message = f"""
            :alert: Look this - :airflow_spin: Airflow DAG: *{dag_name}* | Task: *{task_name}* failed :fail:
            *Execution date*: {execution_date}
            *Error message*: {error_message}
            *DAG URL*: {dag_url}
        """

    slack_notification = SlackWebhookOperator(task_id="slack_notification", http_conn_id=slack_conn_id, message=message)

    return slack_notification.execute(context=context)


def get_spark_job_driver(
    s3_bucket_name: str,
    project_name: str,
    entry_point: str,
    job_args: List[str],
    spark_executor_instances: str = "1",
    spark_executor_cores: str = "1",
    spark_executor_memory: str = "4g",
    spark_driver_cores: str = "1",
    spark_driver_memory: str = "4g",
) -> Dict[str, Any]:
    """Get Spark Job Driver configs.

    Args:
        s3_bucket_name (str): S3 Bucket Name.
        project_name (str): Project Name.
        entry_point (str): Spark Job Entry Point.
        job_args (List[str]): Spark Job Arguments.
        spark_executor_instances (str, optional): Spark Executor Instances. Defaults to "1".
        spark_executor_cores (str, optional): Spark Executor Cores. Defaults to "1".
        spark_executor_memory (str, optional): Spark Executor Memory. Defaults to "4g".
        spark_driver_cores (str, optional): Spark Driver Cores. Defaults to "1".
        spark_driver_memory (str, optional): Spark Driver Memory. Defaults to "4g".

    Returns:
        Dict[str, Any]: Spark Job Driver configs.

    """

    spark_job_driver_configs = {
        "sparkSubmit": {
            "entryPoint": entry_point,
            "entryPointArguments": job_args,
            "sparkSubmitParameters": f"--py-files s3://{s3_bucket_name}/configs/scripts/{project_name}/modules.zip \
                --conf spark.archives=s3://{s3_bucket_name}/configs/scripts/{project_name}/pyspark_deps.tar.gz#environment \
                --conf spark.emr-serverless.driverEnv.PYSPARK_DRIVER_PYTHON=./environment/bin/python \
                --conf spark.emr-serverless.driverEnv.PYSPARK_PYTHON=./environment/bin/python \
                --conf spark.executorEnv.PYSPARK_PYTHON=./environment/bin/python \
                --conf spark.executor.cores={spark_executor_cores} \
                --conf spark.executor.memory={spark_executor_memory} \
                --conf spark.driver.cores={spark_driver_cores} \
                --conf spark.driver.memory={spark_driver_memory} \
                --conf spark.executor.instances={spark_executor_instances} \
                --conf spark.jars=/usr/lib/hudi/hudi-spark-bundle.jar \
                --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
                --conf spark.hadoop.hive.metastore.client.factory.class=com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory",
        }
    }

    return spark_job_driver_configs


def get_spark_configs(s3_bucket: str):
    """Get Spark configs (logs etc).

    Args:
        s3_bucket (str): S3 Bucket Name.

    Returns:
        Dict[str, Any]: Spark configs.

    """

    spark_configuration = {
        "monitoringConfiguration": {"s3MonitoringConfiguration": {"logUri": f"s3://{s3_bucket}/logs"}}
    }

    return spark_configuration
