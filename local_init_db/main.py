import asyncio
import os
import sys
from datetime import datetime
from enum import StrEnum

from dap.api import DAPClient
from dap.dap_types import Credentials
from dap.integration.database import DatabaseConnection
from dap.replicator.sql import SQLReplicator
from dotenv import load_dotenv

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


class InitTableResult(StrEnum):
    COMPLETED = "completed"
    FAILED = "failed"


base_url = os.environ.get("DAP_API_URL")
dap_client_id = os.environ.get("DAP_CLIENT_ID")
dap_client_secret = os.environ.get("DAP_CLIENT_SECRET")
db_connection_string = os.environ.get("DAP_CONNECTION_STRING")
skip_tables = os.environ.get("SKIP_TABLES").split(",")

namespace = "canvas"


async def main():
    start_time = datetime.now()
    print(f"init db started at: {start_time}")

    credentials = Credentials.create(
        client_id=dap_client_id, client_secret=dap_client_secret
    )

    db_connection = DatabaseConnection(connection_string=db_connection_string)

    all_tables = await get_tables(
        api_base_url=base_url, credentials=credentials, namespace=namespace
    )

    tables = [t for t in all_tables if t not in skip_tables]

    for table_name in tables:
        print(f"init table: {table_name} started...")

        result = await init_table(
            table_name=table_name, credentials=credentials, db_connection=db_connection
        )

        print(f"init table: {table_name}, result: {result}")

    end_time = datetime.now()
    print(f"init db finished at: {end_time}")


async def init_table(
    table_name: str, credentials: Credentials, db_connection: DatabaseConnection
):
    result = InitTableResult.COMPLETED

    try:
        async with DAPClient(base_url=base_url, credentials=credentials) as session:
            await SQLReplicator(session=session, connection=db_connection).initialize(
                namespace=namespace, table_name=table_name
            )

    except Exception as e:
        print(f"{table_name} init table exception: {e}")
        result = InitTableResult.FAILED

    return result


async def get_tables(api_base_url: str, credentials: Credentials, namespace: str):
    async with DAPClient(
        base_url=api_base_url,
        credentials=credentials,
    ) as session:
        return await session.get_tables(namespace)


asyncio.run(main())
