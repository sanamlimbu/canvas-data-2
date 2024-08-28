import asyncio
import os
import sys
from enum import StrEnum

from dap.api import DAPClient
from dap.dap_types import Credentials
from dap.integration.database import DatabaseConnection
from dap.integration.database_errors import NonExistingTableError
from dap.replicator.sql import SQLReplicator
from dotenv import load_dotenv


class SyncTableResult(StrEnum):
    INIT_NEEDED = "init_needed"
    COMPLETED = "completed"
    FAILED = "failed"
    NO_TABLE = "no_table"


class InitTableResult(StrEnum):
    COMPLETED = "completed"
    FAILED = "failed"


if len(sys.argv) < 2:
    print(
        "argument missing, enter 'sai-local' | 'sai-supabase' | 'stanley-local | 'stanley-supabase'"
    )
    exit(1)

env_arg = sys.argv[1]

match env_arg:
    case "sai-local":
        load_dotenv(".env.sai.local")
    case "sai-supabase":
        load_dotenv(".env.sai.supabase")
    case "stanley-local":
        load_dotenv(".env.stanley.local")
    case "stanley-supabase":
        load_dotenv(".env.stanley.supabase")
    case _:
        print(
            "invalid argument, enter 'sai-local' | 'sai-supabase' | 'stanley-local | 'stanley-supabase'"
        )
        exit(1)


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

namespace = "canvas"


def main():
    try:
        loop = asyncio.get_event_loop()
    except RuntimeError as e:
        if "There is no current event loop" in str(e):
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
        else:
            raise

    results = loop.run_until_complete(async_main())

    print(results)


async def async_main():
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

        print(f"{result} init table: {table_name}")

    else:
        print(f"{result} sync table: {table_name}")

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
        print(f"{table_name} init_table exception: {e}")
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
        print(f"{table_name} sync_table exception: {e}")
        result = SyncTableResult.FAILED

    return result


main()
