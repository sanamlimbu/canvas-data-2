import asyncio
import os
import sys
from datetime import datetime
from enum import StrEnum

from dap.api import DAPClient
from dap.dap_types import Credentials
from dap.integration.database import DatabaseConnection
from dap.integration.database_errors import NonExistingTableError
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


class SyncTableResult(StrEnum):
    INIT_NEEDED = "init_needed"
    COMPLETED = "completed"
    FAILED = "failed"
    NO_TABLE = "no_table"


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

    loop.run_until_complete(async_main())


async def async_main():
    start_time = datetime.now()
    print(f"sync db started at: {start_time}")

    credentials = Credentials.create(
        client_id=dap_client_id, client_secret=dap_client_secret
    )

    tasks = [
        sync_table(table_name=table_name, credentials=credentials)
        for table_name in tables
    ]

    await asyncio.gather(*tasks)

    end_time = datetime.now()
    print(f"sync db finished at: {end_time}")


async def sync_table(table_name: str, credentials: Credentials):
    print(f"sync table: {table_name} started...")

    db_connection = DatabaseConnection(connection_string=db_connection_string)

    result = SyncTableResult.COMPLETED

    try:
        async with DAPClient(base_url=base_url, credentials=credentials) as session:
            await SQLReplicator(session=session, connection=db_connection).synchronize(
                namespace=namespace, table_name=table_name
            )

    except NonExistingTableError as e:
        result = SyncTableResult.NO_TABLE

    except ValueError as e:
        if "table not initialized" in str(e):
            result = SyncTableResult.INIT_NEEDED

    except Exception as e:
        print(f"{table_name} sync table exception: {e}")
        result = SyncTableResult.FAILED

    print(f"sync table: {table_name}, result: {result}")

    return result


main()
