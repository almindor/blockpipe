use dotenv::dotenv;
use std::env;
use web3::transports::{Http, Ipc};

mod pipe;
mod sql;

use crate::pipe::Pipe;
use crate::sql::SqlOperation;

async fn connect_to_ipc(path: &str) -> Ipc {
    loop {
        let result = Ipc::new(path).await;
        match result {
            Ok(transport) => return transport,
            Err(err) => panic!("error on transport creation {}", err),
        }
    }
}

#[tokio::main]
async fn main() {
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
    let pg_path = env::var("PG_PATH").expect("PG_PATH env var should be provided");
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

    let (sleep_s, sleep_r) = crossbeam_channel::bounded(0);

    if let Ok(path) = ipc_path {
        let transport = connect_to_ipc(&path).await;
        let mut pipe = Pipe::new(transport, &pg_path, operation, labo, sleep_r)
            .await
            .unwrap();
        std::process::exit(pipe.run(sleep_s).await.unwrap());
    } else if let Ok(path) = http_path {
        let transport = Http::new(&path).expect("HTTP connection failed");
        let mut pipe = Pipe::new(transport, &pg_path, operation, labo, sleep_r)
            .await
            .unwrap();
        std::process::exit(pipe.run(sleep_s).await.unwrap());
    } else {
        panic!("IPC_PATH or RPC_PATH should be provided");
    }
}
