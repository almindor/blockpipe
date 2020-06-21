use postgres::{Client, NoTls};
use std::{thread, time};

use std;
use std::fmt::Write;
use std::string::String;

use crate::sql;
use crate::sql::{Sequelizable, SqlOperation};
use web3;
use web3::futures::Future;
use web3::transports::EventLoopHandle;
use web3::types::{Block, BlockId, SyncState, Transaction, U64};
use web3::Transport;
use web3::Web3;

use log::{info, trace};
use simple_signal::{self, Signal};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

mod error;

const MAX_BLOCKS_PER_BATCH: i32 = 100;

#[allow(dead_code)]
pub struct Pipe<T: Transport> {
    eloop: EventLoopHandle, // needs to be held for event loop to be owned right
    web3: Web3<T>,
    pg_client: Client,
    last_db_block: u64, // due to BIGINT and lack of NUMERIC support in driver
    last_node_block: u64,
    syncing: bool,
    operation: SqlOperation,
}

impl<T: Transport> Pipe<T> {
    const ONE_MINUTE: time::Duration = time::Duration::from_secs(60);

    // we want to return error here if db fails
    #[allow(clippy::new_ret_no_self)]
    pub fn new(
        transport: T,
        eloop: EventLoopHandle,
        pg_path: &str,
        op: SqlOperation,
        last_block_override: i64,
    ) -> Result<Pipe<T>, Box<dyn std::error::Error>> {
        let mut pg_client = Client::connect(pg_path, NoTls)?;

        let rows = pg_client.query(sql::LAST_DB_BLOCK_QUERY, &[])?;
        let mut last_db_block_number = match rows.iter().next() {
            Some(row) => row.get(0),
            None => 0,
        };

        if last_block_override > last_db_block_number {
            last_db_block_number = last_block_override;
        }

        let web3 = Web3::new(transport);

        Ok(Pipe {
            eloop,
            web3,
            pg_client,
            last_db_block: last_db_block_number as u64,
            last_node_block: 0,
            syncing: false,
            operation: op,
        })
    }

    fn update_node_info(&mut self) -> Result<bool, web3::Error> {
        info!("Getting info from eth node.");
        let last_block_number = self.web3.eth().block_number().wait()?.as_u64();
        let syncing = self.web3.eth().syncing().wait()?;

        self.last_node_block = last_block_number;
        self.syncing = syncing != SyncState::NotSyncing;
        Ok(true)
    }

    fn sleep_with_msg(msg: &str) {
        info!("{}", msg);
        thread::sleep(Self::ONE_MINUTE);
    }

    fn sleep_when_syncing(&self) -> bool {
        if self.syncing {
            Self::sleep_with_msg("Node is syncing, sleeping for a minute.");
            return true;
        }

        false
    }

    fn write_insert_header<S: Sequelizable>(
        mut sql_query: &mut String,
    ) -> Result<(), std::fmt::Error> {
        writeln!(
            &mut sql_query,
            "INSERT INTO {}({}) VALUES",
            S::table_name(),
            S::insert_fields()
        )
    }

    fn print_copy_header<S: Sequelizable>() {
        info!(
            "COPY {}({}) FROM STDIN NULL 'NULL'",
            S::table_name(),
            S::insert_fields()
        );
    }

    fn trim_ends(sql_query: &mut String) {
        sql_query.pop(); // remove \n
        sql_query.pop(); // remove ,
    }

    fn store_next_batch(&mut self, running: &Arc<AtomicBool>) -> Result<i32, error::PipeError> {
        let mut next_block_number = self.last_db_block + 1;
        let mut processed: i32 = 0;
        let mut processed_tx: i32 = 0;
        let mut sql_blocks: String = String::with_capacity(1096 * 1024 * 10);
        let mut data_transactions: String = String::with_capacity(4096 * 1024 * 10);

        Self::write_insert_header::<Block<Transaction>>(&mut sql_blocks)?;

        if self.operation == SqlOperation::Insert {
            Self::write_insert_header::<Transaction>(&mut data_transactions)?;
        }

        trace!("Getting blocks");
        while processed < MAX_BLOCKS_PER_BATCH
            && next_block_number <= self.last_node_block
            && running.load(Ordering::SeqCst)
        {
            #[cfg(feature = "timing")]
            let start = PreciseTime::now();

            trace!("Getting block #{}", next_block_number);
            let block = self
                .web3
                .eth()
                .block_with_txs(BlockId::from(U64::from(next_block_number)))
                .wait()?
                .unwrap();

            trace!("Got block #{}", next_block_number);
            next_block_number += 1;
            processed += 1;

            writeln!(&mut sql_blocks, "{}", block.to_insert_values())?;

            trace!("Getting transactions for block");
            for tx in block.transactions.iter() {
                writeln!(&mut data_transactions, "{}", tx.to_values(&self.operation))?;
                processed_tx += 1;
            }
            trace!("Got {} transactions", processed_tx);
        }

        if processed == 0 {
            return Ok(0);
        }
        Self::trim_ends(&mut sql_blocks);

        let mut pg_tx = self.pg_client.transaction()?;
        // save the blocks

        trace!("Storing {} blocks to DB using insert", processed);
        pg_tx.execute(sql_blocks.as_str(), &[])?;

        if processed_tx > 0 {
            match self.operation {
                SqlOperation::Insert => {
                    trace!("Storing {} transactions to DB using insert", processed_tx);
                    // upsert in case of reorg
                    Self::trim_ends(&mut data_transactions);
                    write!(&mut data_transactions, "\nON CONFLICT (hash) DO UPDATE SET nonce = excluded.nonce, blockHash = excluded.blockHash, blockNumber = excluded.blockNumber, transactionIndex = excluded.transactionIndex, \"from\" = excluded.from, \"to\" = excluded.to, \"value\" = excluded.value, gas = excluded.gas, gasPrice = excluded.gasPrice")?;
                    pg_tx.execute(data_transactions.as_str(), &[])?;
                    trace!("Commiting direct DB operations");
                    pg_tx.commit()?;
                }
                SqlOperation::Copy => {
                    trace!("Commiting direct DB operations");
                    pg_tx.commit()?;
                    trace!("Storing {} transactions to DB using copy", processed_tx);
                    print!("{}", data_transactions);
                }
            }
        } else {
            trace!("Commiting direct DB operations");
            pg_tx.commit()?;
        }

        self.last_db_block = next_block_number - 1;
        info!(
            "Processed {} blocks. At {}/{}",
            processed, self.last_db_block, self.last_node_block
        );

        Ok(processed)
    }

    pub fn main(&mut self, running: &Arc<AtomicBool>) -> Result<i32, error::PipeError> {
        self.update_node_info()?;
        if self.sleep_when_syncing() {
            return Ok(0);
        }

        info!(
            "Queue size: {}\nlast_db_block: {}, last_node_block: {}",
            self.last_node_block - self.last_db_block,
            self.last_db_block,
            self.last_node_block
        );

        match self.operation {
            SqlOperation::Insert => {}
            SqlOperation::Copy => Self::print_copy_header::<Transaction>(),
        }

        while self.last_db_block < self.last_node_block && running.load(Ordering::SeqCst) {
            self.store_next_batch(running)?;
        }

        if running.load(Ordering::SeqCst) {
            Self::sleep_with_msg("Run done, sleeping for one minute.");
        }

        Ok(0)
    }

    pub fn run(&mut self) -> Result<i32, error::PipeError> {
        let running = Arc::new(AtomicBool::new(true));
        let r = running.clone();
        simple_signal::set_handler(&[Signal::Int, Signal::Term], move |_signals| {
            info!("Exiting...");
            r.store(false, Ordering::SeqCst);
        });

        let mut iteration: u64 = 0;
        while running.load(Ordering::SeqCst) {
            iteration += 1;
            trace!("Main loop iteration #{}", iteration);
            self.main(&running)?;
        }

        Ok(0)
    }
}
