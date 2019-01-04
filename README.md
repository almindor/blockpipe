# blockpipe

Rust block piper that dumps ethereum block/transaction info to postgres.

## Setup

Run `sql/tables.sql` inside a newly created `blockpipe` postgres DB.
Requires parity to be running with IPC api `eth` enabled.

## Environment

Uses dotenv to set up it's env vars. Place `IPC_PATH` and `PG_PATH` in environment or in `.env` file.

You can also define `OPERATION=copy` to switch into `copy mode` which will emit transaction data in format compatible with PSQL `COPY FROM STDIN NULL 'NULL'` command. This can be used with piping the output into `psql` to considerably speed up the operation.
