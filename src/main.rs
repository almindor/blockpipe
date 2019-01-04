extern crate dotenv;
extern crate postgres;
extern crate web3;
extern crate simple_signal;

#[macro_use]
extern crate log;
extern crate stderrlog;

mod pipe;
mod sql;

use dotenv::dotenv;
use pipe::Pipe;
use std::env;
use web3::transports::{Http, Ipc};
use sql::SqlOperation;

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
    let mut operation = SqlOperation::Insert;

    if let Ok(env_op) = env::var("OPERATION") {
        if env_op == "copy" {
            operation = SqlOperation::Copy;
        }
    }

    if let Ok(path) = ipc_path {
        let (eloop, transport) =
            Ipc::new(&path).expect("IPC connection failed");
        let mut pipe = Pipe::new(transport, eloop, &pg_path, operation).unwrap();
        pipe.run().unwrap();
    } else if let Ok(path) = http_path {
        let (eloop, transport) =
            Http::new(&path).expect("HTTP connection failed");
        let mut pipe = Pipe::new(transport, eloop, &pg_path, operation).unwrap();
        pipe.run().unwrap();
    } else {
        panic!("IPC_PATH or RPC_PATH should be provided");
    }
}
