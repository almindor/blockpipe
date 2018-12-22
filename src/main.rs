extern crate dotenv;
extern crate postgres;
extern crate web3;

mod pipe;
mod sql;

use dotenv::dotenv;
use pipe::Pipe;
use std::env;
use web3::transports::{Http, Ipc};

fn main() {
    dotenv().ok();
    // main env var, panic if missing
    let pg_path =
        env::var("PG_PATH").expect("IPC_PATH env var should be provided");
    let ipc_path = env::var("IPC_PATH");
    let http_path = env::var("RPC_PATH");

    if let Ok(path) = ipc_path {
        let (eloop, transport) =
            Ipc::new(&path).expect("IPC connection failed");
        let mut pipe = Pipe::new(transport, eloop, &pg_path).unwrap();
        pipe.run().unwrap();
    } else if let Ok(path) = http_path {
        let (eloop, transport) =
            Http::new(&path).expect("HTTP connection failed");
        let mut pipe = Pipe::new(transport, eloop, &pg_path).unwrap();
        pipe.run().unwrap();
    } else {
        panic!("IPC_PATH or RPC_PATH should be provided");
    }
}
