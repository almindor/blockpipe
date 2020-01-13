use dotenv::dotenv;
use std::env;
use web3::transports::{EventLoopHandle, Http, Ipc};

mod pipe;
mod sql;

use crate::pipe::Pipe;
use crate::sql::SqlOperation;

fn connect_to_ipc(path: &str) -> (EventLoopHandle, Ipc) {
    loop {
        match Ipc::new(path) {
            Ok(result) => return result,
            Err(err) => {
                eprintln!("IPC connection failed with: {}, retrying in 5s", err)
            }
        }

        std::thread::sleep(std::time::Duration::from_secs(5));
    }
}

fn main() {
    let timestamps = if cfg!(debug_assertions) {
        stderrlog::Timestamp::Millisecond
    } else {
        stderrlog::Timestamp::Off
    };

    stderrlog::new()
        .module(module_path!())
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
    let labo = match env::var("LABO") {
        Ok(val) => val.parse::<i64>().unwrap(),
        Err(_) => 0,
    };
    let mut operation = SqlOperation::Insert;

    if let Ok(env_op) = env::var("OPERATION") {
        if env_op == "copy" {
            operation = SqlOperation::Copy;
        }
    }

    if let Ok(path) = ipc_path {
        let (eloop, transport) = connect_to_ipc(&path);
        let mut pipe =
            Pipe::new(transport, eloop, &pg_path, operation, labo).unwrap();
        pipe.run().unwrap();
    } else if let Ok(path) = http_path {
        let (eloop, transport) =
            Http::new(&path).expect("HTTP connection failed");
        let mut pipe =
            Pipe::new(transport, eloop, &pg_path, operation, labo).unwrap();
        pipe.run().unwrap();
    } else {
        panic!("IPC_PATH or RPC_PATH should be provided");
    }
}
