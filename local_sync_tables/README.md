# Canvas Data 2 Sync From Local Device

Synchronize Canvas Data 2 from a local device to your choice of database.

# Initialize

Make sure to initialize tables before synchronizing. To initialize go to `local_init_db` folder and set tables as env.

## Dependencies

The project uses the dependencies listed in the `requirements.txt` file. Install them with:

```
pip install -r requirements.txt
```

## Environment variables

Create four env files at root of this folder.

- `.env.sai.local`
- `.env.sai.supabase`
- `.env.stanley.local`
- `.env.stanley.supabase`

Each file should contain the following variables:

```
DAP_API_URL=""
DAP_CLIENT_ID=""
DAP_CLIENT_SECRET=""
DAP_CONNECTION_STRING=""
TABLES=""
```

## Run with command args

The following arguments are supported:

- `sai-local`: PostgreSQL on a local device
- `sai-supabase`: PostgreSQL on Supabase
- `stanley-local`: PostgreSQL on a local device
- `stanley-supabase`: PostgreSQL on Supabase

Example commands:

```
python main.py sai-local
pyton main.py sai-supabase
python main.py stanley-local
pyton main.py stanley-supabase
```
