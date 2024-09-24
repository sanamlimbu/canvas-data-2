# Canvas Data 2 Sync Workflow

Synchronize Canvas Data 2 using AWS Lambda to Supabase. Lambda function folder is `sync_tables`.

# Initialize

Make sure to initialize tables before synchronizing. To initialize go to `local_init_db` folder and set tables as env.

## Dependencies

The project uses the dependencies listed in the `requirements.txt` file of `sync_tables` folder.

```
aws-lambda-powertools==2.34.2
instructure-dap-client[postgresql]==1.1.0
boto3
```

## Environment variables

Lambda function uses following variables:

```
DAP_API_URL=""
DAP_CLIENT_ID=""
DAP_CLIENT_SECRET=""
DAP_CONNECTION_STRING=""
TABLES=""
SNS_TOPIC_ARN=""
```

## Terrafrom

There are two terraform cloud deployments: `sai` and `stanley`. Configure required variables in `variables.tf` file.
