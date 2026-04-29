# 327-03-Assignment-8

TCP client/server project for querying IoT data from a distributed, partially replicated database setup.

## Files

- `echo_server.py` - starts the server and returns formatted query results.
- `echo_client.py` - prompts for a query and sends it to the server.
- `db.py` - database helper functions and aggregation logic.

## Requirements

- Python 3.10 or newer.
- Access to a PostgreSQL database that contains a table named `metadata_virtual`.
- A partner PostgreSQL database URL if you want the server to query historical data from a teammate's database.

Install the Python packages used by the project:

```bash
pip install psycopg2-binary python-dotenv tzdata
```

## Environment setup

The project reads settings from `.env.local` in the repository root.

Example `.env.local`:

```env
LOCAL_DATABASE_URL=postgresql://user:password@host/dbname?sslmode=require
PARTNER_DATABASE_URL=postgresql://user:password@host/dbname?sslmode=require
HOUSE_A_NAME=Brandon's House
HOUSE_B_NAME=Ryan's House
HOUSE_A_TOPICS=bsamson1024@gmail.com/myIOTdevice
HOUSE_B_TOPICS=vuryan782@gmail.com/home
SHARING_START_UTC=2026-04-20T00:00:00Z
```

## Database expectations

The code expects `metadata_virtual` to provide at least these columns:

- `id`
- `topic`
- `time`
- `payload`

The payload is parsed as JSON when possible. The current query logic looks for moisture, water, and current/electricity values using common field names such as:

- `Moisture Meter - moisture_level`
- `moisture_level`
- `humidity`
- `current_usage`
- `ammeter`
- `water_usage`
- `water_consumption`

## Running the server

Open a terminal in the repository folder and run:

```bash
py echo_server.py
```

The server will prompt for a port number. A common local choice is `5000`.

Example:

```text
Enter server port (1-65535): 5000
```

After startup, the server listens on `0.0.0.0:<port>` and waits for one client connection.

## Running the client

Open a second terminal in the same folder and run:

```bash
py echo_client.py
```

When prompted:

```text
Enter server IP address or hostname: 127.0.0.1
Enter server port (1-65535): 5000
```

Then choose one of the supported query numbers:

- `1` - average moisture inside kitchen fridges
- `2` - average water consumption per cycle
- `3` - which house consumed more electricity in the past 24 hours
- `quit` - close the client connection


### How the system connects to and retrieves data from the relevant sources

The server connects to PostgreSQL using the database URLs stored in `.env.local`. `LOCAL_DATABASE_URL` points to the local database, and `PARTNER_DATABASE_URL` points to the teammate's database when partner history is needed. The query logic reads from the `metadata_virtual` table and filters rows by `topic` and by the requested time window. The `topic` field is used to decide which rows belong to House A and which rows belong to House B.

### How distributed query processing was implemented

Distributed query processing is handled in `db.py`. The code first reads rows from the local database for the requested time window. If the requested window overlaps the agreed sharing cutoff, the code also queries the partner database for the portion of the window that predates sharing. After that, the system merges the results into one list of readings and performs the final aggregation in Python.

### How query completeness was determined

The system uses `SHARING_START_UTC` to decide whether the local database should already contain a complete answer. If the requested window starts after sharing began, the code assumes the local replicated data should be enough. If the window begins before sharing started, the code treats the result as potentially incomplete and queries the partner database for missing historical data. The server also prints a coverage note so users know whether the answer came from local data only or from a combination of local and partner sources.

### How DataNiz metadata and data sharing were used

The project uses DataNiz metadata in the `metadata_virtual` table. The `topic` field identifies the device or house that produced each row, and the `payload` field holds the actual sensor values. The code uses payload key lists such as `MOISTURE_KEYS`, `CURRENT_KEYS`, and `WATER_KEYS` to extract the correct measurement from each row. Data sharing is modeled by using the same topic structure across both databases and by using `SHARING_START_UTC` to separate original data from replicated shared data. 
