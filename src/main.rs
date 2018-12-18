extern crate dotenv;
extern crate postgres;
extern crate web3;

mod pipe;
mod sql;

use dotenv::dotenv;
use pipe::Pipe;
use std::env;

fn main() {
    dotenv().ok();
    // main env var, panic if missing
    let ipc_path = env::var("IPC_PATH").expect("IPC_PATH env var not provided");
    let pg_path = env::var("PG_PATH").expect("IPC_PATH env var not provided");

    let mut pipe = Pipe::new(&ipc_path, &pg_path).unwrap();
    pipe.run().unwrap();
}
