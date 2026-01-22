# SQLcl Connections Directory

This directory is mounted into the Docker container to provide SQLcl with pre-configured Oracle database connections.

## Setup Instructions

### Option 1: Copy from your local SQLcl installation

If you have SQLcl installed locally with connections already configured:

**Windows:**
```bash
copy %USERPROFILE%\.dbtools\connections.json .\.dbtools\
```

**Linux/Mac:**
```bash
cp ~/.dbtools/connections.json ./.dbtools/
```

### Option 2: Create connections manually

1. Install SQLcl locally from Oracle
2. Run SQLcl and create a connection:
   ```sql
   conn -save TestUserCSV TestUserCSV/TestPassword123@localhost:1521/XEPDB1
   ```
3. Copy the generated `connections.json` to this directory

### Option 3: Use environment variable

Set `SQLCL_CONNECTIONS_PATH` in your `.env` file to point to your existing `.dbtools` directory:

```env
SQLCL_CONNECTIONS_PATH=C:/Users/YourName/.dbtools
```

## Connection Format

The `connections.json` file contains encrypted connection information. SQLcl manages this file automatically when you save connections.

## Troubleshooting

If you see "Connection not found: TestUserCSV" error:
1. Ensure `connections.json` exists in this directory
2. Verify the connection name matches `ORACLE_DEFAULT_CONNECTION` in your `.env`
3. Check that the connection was created with the same SQLcl version
