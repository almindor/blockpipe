# blockpipe

Rust block piper that dumps ethereum block/transaction info to postgres.

## Setup

Run `sql/tables.sql` inside a newly created `blockpipe` postgres DB.
Requires parity to be running with IPC api `eth` enabled.

## Environment

Uses dotenv to set up it's env vars. Place `IPC_PATH` and `PG_PATH` in environment or in `.env` file.
