use dotenv::dotenv;
use std::env;
use web3::transports::{Http, Ipc};

mod pipe;
mod sql;

use crate::pipe::Pipe;
use crate::sql::SqlOperation;

fn main() {
    let timestamps = if cfg!(debug_assertions) {
        stderrlog::Timestamp::Millisecond
    } else {
        stderrlog::Timestamp::Off
    };

    stderrlog::new().module(module_path!())
        .verbosity(255) // controlled by compile time feature definitions
        .timestamp(timestamps)
        .init()
        .unwrap();

    dotenv().ok();
    // main env var, panic if missing
    let pg_path =
        env::var("PG_PATH").expect("PG_PATH env var should be provided");
    let ipc_path = env::var("IPC_PATH");
    let http_path = env::var("RPC_PATH");
    let labo = if env::var("LABO").is_ok() {
        env::var("LABO").unwrap().parse::<i64>().unwrap()
    } else {
        0
    };
    let mut operation = SqlOperation::Insert;

    if let Ok(env_op) = env::var("OPERATION") {
        if env_op == "copy" {
            operation = SqlOperation::Copy;
        }
    }

    if let Ok(path) = ipc_path {
        let (eloop, transport) =
            Ipc::new(&path).expect("IPC connection failed");
        let mut pipe = Pipe::new(transport, eloop, &pg_path, operation, labo).unwrap();
        pipe.run().unwrap();
    } else if let Ok(path) = http_path {
        let (eloop, transport) =
            Http::new(&path).expect("HTTP connection failed");
        let mut pipe = Pipe::new(transport, eloop, &pg_path, operation, labo).unwrap();
        pipe.run().unwrap();
    } else {
        panic!("IPC_PATH or RPC_PATH should be provided");
    }
}
