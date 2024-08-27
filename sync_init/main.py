import asyncio
import os
from enum import StrEnum

import boto3
from aws_lambda_powertools import Logger
from aws_lambda_powertools.utilities.typing import LambdaContext
from dap.api import DAPClient
from dap.dap_types import Credentials
from dap.integration.database import DatabaseConnection
from dap.integration.database_errors import NonExistingTableError
from dap.replicator.sql import SQLReplicator


class SyncTableResult(StrEnum):
    INIT_NEEDED = "init_needed"
    COMPLETED = "completed"
    FAILED = "failed"
    NO_TABLE = "no_table"


class InitTableResult(StrEnum):
    COMPLETED = "completed"
    FAILED = "failed"


base_url = os.environ.get("DAP_API_URL")
dap_client_id = os.environ.get("DAP_CLIENT_ID")
dap_client_secret = os.environ.get("DAP_CLIENT_SECRET")
db_connection_string = os.environ.get("DAP_CONNECTION_STRING")
tables = os.environ.get("TABLES").split(",")
sns_topic_arn = os.environ.get("SNS_TOPIC_ARN")

namespace = "canvas"

logger = Logger()

client = boto3.client("sns")


def lambda_handler(event, context: LambdaContext):
    os.chdir("/tmp/")

    loop = asyncio.get_event_loop()

    results = loop.run_until_complete(main())

    message = f"{results}"

    response = client.publish(
        TopicArn=sns_topic_arn,
        Message=message,
        Subject="Canvas Data 2 Sync Results",
    )

    logger.info(f"published results to SNS: {response}")

    return event


async def main():
    tasks = [
        sync_or_init_table(
            table_name=table_name,
        )
        for table_name in tables
    ]

    results = await asyncio.gather(*tasks)

    formatted_results = [
        {"Table": result["table_name"], "Result": result["result"].value}
        for result in results
    ]

    return formatted_results


async def sync_or_init_table(table_name: str):
    result = await sync_table(table_name=table_name)

    if result == SyncTableResult.INIT_NEEDED:
        result = await init_table(table_name=table_name)

        logger.info(f"{result} init table: {table_name}")

    else:
        logger.info(f"{result} sync table: {table_name}")

    return {"table_name": table_name, "result": result}


async def init_table(table_name: str):
    result = InitTableResult.COMPLETED

    credentials = Credentials.create(
        client_id=dap_client_id, client_secret=dap_client_secret
    )

    connection = DatabaseConnection(connection_string=db_connection_string)

    try:
        async with DAPClient(base_url=base_url, credentials=credentials) as session:
            await SQLReplicator(session=session, connection=connection).initialize(
                namespace=namespace, table_name=table_name
            )

    except Exception as e:
        logger.exception(f"{table_name} init_table exception: {e}")
        result = InitTableResult.FAILED

    return result


async def sync_table(table_name: str):
    result = SyncTableResult.COMPLETED

    credentials = Credentials.create(
        client_id=dap_client_id, client_secret=dap_client_secret
    )

    connection = DatabaseConnection(connection_string=db_connection_string)

    try:
        async with DAPClient(base_url=base_url, credentials=credentials) as session:
            await SQLReplicator(session=session, connection=connection).synchronize(
                namespace=namespace, table_name=table_name
            )

    except NonExistingTableError as e:
        result = SyncTableResult.NO_TABLE

    except ValueError as e:
        if "table not initialized" in str(e):
            result = SyncTableResult.INIT_NEEDED

    except Exception as e:
        logger.exception(f"{table_name} sync_table exception: {e}")
        result = SyncTableResult.FAILED

    return result
