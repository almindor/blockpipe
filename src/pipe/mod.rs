use postgres::{Connection, TlsMode};
use std::{thread, time};

use std;
use std::fmt::Write;
use std::string::String;

use sql;
use sql::Sequelizable;
use web3;
use web3::futures::Future;
use web3::transports::{EventLoopHandle, Ipc};
use web3::types::{Block, BlockId, SyncState, Transaction};
use web3::Web3;

mod error;

const MAX_BLOCKS_PER_BATCH: i32 = 100;

#[allow(dead_code)]
pub struct Pipe {
    eloop: EventLoopHandle, // needs to be held for event loop to be owned right
    web3: Web3<Ipc>,
    pg_client: Connection,
    last_db_block: u64, // due to BIGINT and lack of NUMERIC support in driver
    last_node_block: u64,
    syncing: bool,
}

impl Pipe {
    const ONE_MINUTE: time::Duration = time::Duration::from_secs(60);

    pub fn new(ipc_path: &str, pg_path: &str) -> Result<Pipe, Box<std::error::Error>> {
        let pg_client = Connection::connect(pg_path, TlsMode::None)?;
        let (eloop, transport) = Ipc::new(ipc_path)?;

        let rows = pg_client.query(sql::LAST_DB_BLOCK_QUERY, &[])?;
        let last_db_block_number: i64 = match rows.iter().next() {
            Some(row) => row.get(0),
            None => 0,
        };
        let web3 = Web3::new(transport);

        Ok(Pipe {
            eloop: eloop,
            web3: web3,
            pg_client: pg_client,
            last_db_block: last_db_block_number as u64,
            last_node_block: 0,
            syncing: false,
        })
    }

    fn update_node_info(&mut self) -> Result<bool, web3::Error> {
        println!("Getting info from eth node.");
        let last_block_number = self.web3.eth().block_number().wait()?.as_u64();
        let syncing = self.web3.eth().syncing().wait()?;

        self.last_node_block = last_block_number;
        self.syncing = syncing != SyncState::NotSyncing;
        Ok(true)
    }

    fn sleep_with_msg(msg: &str) {
        println!("{}", msg);
        thread::sleep(Pipe::ONE_MINUTE);
    }

    fn sleep_when_syncing(&self) -> bool {
        if self.syncing {
            Pipe::sleep_with_msg("Node is syncing, sleeping for a minute.");
            return true;
        }

        false
    }

    fn write_insert_header<T: Sequelizable>(
        mut sql_query: &mut String,
    ) -> Result<(), std::fmt::Error> {
        write!(
            &mut sql_query,
            "INSERT INTO {}({}) VALUES\n",
            T::table_name(),
            T::insert_fields()
        )
    }

    fn trim_ends(sql_query: &mut String) {
        sql_query.pop(); // remove \n
        sql_query.pop(); // remove ,
    }

    fn store_next_batch(&mut self) -> Result<i32, error::PipeError> {
        let mut next_block_number = self.last_db_block + 1;
        let mut processed: i32 = 0;
        let mut processed_tx: i32 = 0;
        let mut sql_blocks: String = String::with_capacity(1096 * 1024 * 10);
        let mut sql_transactions: String = String::with_capacity(4096 * 1024 * 10);

        Pipe::write_insert_header::<Block<Transaction>>(&mut sql_blocks)?;
        Pipe::write_insert_header::<Transaction>(&mut sql_transactions)?;

        while processed < MAX_BLOCKS_PER_BATCH && next_block_number <= self.last_node_block {
            let block = self
                .web3
                .eth()
                .block_with_txs(BlockId::from(next_block_number))
                .wait()?
                .unwrap();
            next_block_number += 1;
            processed += 1;

            write!(&mut sql_blocks, "({}),\n", block.to_insert_values())?;

            for tx in block.transactions.iter() {
                write!(&mut sql_transactions, "({}),\n", tx.to_insert_values())?;
                processed_tx += 1;
            }
        }

        if processed == 0 {
            return Ok(0);
        }
        Pipe::trim_ends(&mut sql_blocks);
        Pipe::trim_ends(&mut sql_transactions);
        // upsert in case of reorg
        write!(&mut sql_transactions, "\nON CONFLICT (hash) DO UPDATE SET nonce = excluded.nonce, blockHash = excluded.blockHash, blockNumber = excluded.blockNumber, transactionIndex = excluded.transactionIndex, \"from\" = excluded.from, \"to\" = excluded.to, \"value\" = excluded.value, gas = excluded.gas, gasPrice = excluded.gasPrice")?;

        let pg_tx = self.pg_client.transaction()?;
        // save the blocks
        pg_tx.execute(&sql_blocks, &[])?;

        if processed_tx > 0 {
            pg_tx.execute(&sql_transactions, &[])?;
        }
        pg_tx.commit()?;

        self.last_db_block = next_block_number - 1;
        println!(
            "Processed {} blocks. At {}/{}",
            processed, self.last_db_block, self.last_node_block
        );
        Ok(processed)
    }

    pub fn run(&mut self) -> Result<i32, error::PipeError> {
        loop {
            self.update_node_info()?;
            if self.sleep_when_syncing() {
                continue;
            }

            println!("Queue size: {}", self.last_node_block - self.last_db_block);

            while self.last_db_block < self.last_node_block {
                self.store_next_batch()?;
            }

            Pipe::sleep_with_msg("Run done, sleeping for one minute.")
        }
    }
}
