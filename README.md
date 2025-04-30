# Salesforce Sync Lambdas  

This AWS Lambda functions extracts data from a SQL Server view related to specific entities, transforms it into a Salesforce-compatible format, and synchronizes it with Salesforce using their REST API. It runs daily at 5:00 AM via Amazon EventBridge, as part of a 3-Lambda suite:  

- terceros -> synchronizes Accounts
- sucursales -> synchronizes RetailStores
- contacts -> synchronizes Acuerdos

## Project Structure

```powershell
├── main.py
├── config/
│   └── config_loader.py
├── services/
│   ├── dynamo_service.py
│   ├── database_service.py
│   ├── mapper_service.py
│   └── salesforce_service.py
└── constants/
    └── allowed_companies.py
```

| Module                    | Responsibility                                            |
| --------                  | -------                                                   |
| main.py                   | Lambda entrypoint and orchestrator                        |
| config_service.py         | Loads and validates config from config.json               |
| database_service.py       | Connects to SQL Server and fetches view results           |
| mapper_service.py         | Transforms SQL rows into Salesforce-compatible payloads   |
| salesforce_service.py     | Authenticates and interacts with Salesforce               |
| dynamo_service.py         | Reads/writes Salesforce ID mappings from DynamoDB         |
| allowed_companies.py      | Whitelists valid selected_company values                  |

## Architecture

This will be deployed in a AWS instance with the current structure

![Diagrama arquitectura](/static/diagram.jpg)

## Configuration

All settings are read from a config.json file, bundled with the Lambda:

```json
{
  "VIEW": "SELECT * FROM sucursales_view WHERE ...",
  "AUTH_URL": "https://login.salesforce.com/services/oauth2/token",
  "AUTH_PARAMS": {
    "grant_type": "password",
    "client_id": "...",
    "client_secret": "...",
    "username": "...",
    "password": "..."
  },
  "API_URI": "/services/data/v58.0/sobjects/ti_sucursal__c/"
}
```

## Execution Flow

1. Triggered by EventBridge at 5:00 AM UTC.
2. Loads config.json and validates presence of all required keys.
3. Receives input event with a selected_company value (CASABLANCA, ITALCOL, SANTA_REYES).
4. Connects to the corresponding SQL database and executes the configured view.
5. Maps each SQL result row into a Salesforce-compatible JSON object.
6. Authenticates with Salesforce using password grant type.
7. For each retail store entry:
   - Retrieves the matching Account ID from DynamoDB.
   - Checks if the store already exists in Salesforce (via external_id).
   - If not exists: creates a new record (POST).
   - If exists: updates the record (PATCH).
   - Updates the related DynamoDB entry to track sucursales associated with the account.
8. Returns a summary of successful and failed operations.

## Scheduling

This Lambda is scheduled using Amazon EventBridge with a cron expression:

```ps
cron(0 5 * * ? *)
```

This executes the function daily at 5:00 AM UTC.

## Environment Variables

| Variable                    | Description                         |
| --------                    | -------                             |
| DB_HOST                     | Default database host               |
| DB_HOST_ITALCOL             | Alternate host for ITALCOL company  |
| DB_NAME, DB_NAME1, DB_NAME2 | Database names for each company     |
| DB_USER                     | SQL Server username                 |
| DB_PASSWORD                 | SQL Server password                 |
