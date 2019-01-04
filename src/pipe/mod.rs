use postgres::{Connection, TlsMode};
use std::{thread, time};

use std;
use std::fmt::Write;
use std::string::String;

use sql;
use sql::{Sequelizable, SqlOperation};
use web3;
use web3::futures::Future;
use web3::transports::EventLoopHandle;
use web3::types::{Block, BlockId, SyncState, Transaction};
use web3::Transport;
use web3::Web3;

use simple_signal::{self, Signal};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;


#[cfg(feature="timing")]
use time::{PreciseTime, Duration};

mod error;

const MAX_BLOCKS_PER_BATCH: i32 = 100;

#[allow(dead_code)]
pub struct Pipe<T: Transport> {
    eloop: EventLoopHandle, // needs to be held for event loop to be owned right
    web3: Web3<T>,
    pg_client: Connection,
    last_db_block: u64, // due to BIGINT and lack of NUMERIC support in driver
    last_node_block: u64,
    syncing: bool,
    operation: SqlOperation,
}

impl<T: Transport> Pipe<T> {
    const ONE_MINUTE: time::Duration = time::Duration::from_secs(60);

    pub fn new(
        transport: T,
        eloop: EventLoopHandle,
        pg_path: &str,
        op: SqlOperation
    ) -> Result<Pipe<T>, Box<std::error::Error>> {
        let pg_client = Connection::connect(pg_path, TlsMode::None)?;

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
        write!(
            &mut sql_query,
            "INSERT INTO {}({}) VALUES\n",
            S::table_name(),
            S::insert_fields()
        )
    }

    fn print_copy_header<S: Sequelizable>() {
        info!(
            "COPY {}({}) FROM STDIN NULL 'NULL'\n",
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
        let mut data_transactions: String =
            String::with_capacity(4096 * 1024 * 10);

        #[cfg(feature="timing")]
        let mut average_duration = Duration::zero();

        Self::write_insert_header::<Block<Transaction>>(&mut sql_blocks)?;

        if self.operation == SqlOperation::Insert {
            Self::write_insert_header::<Transaction>(&mut data_transactions)?;
        }

        trace!("Getting blocks");
        while processed < MAX_BLOCKS_PER_BATCH
            && next_block_number <= self.last_node_block
            && running.load(Ordering::SeqCst)
        {
            #[cfg(feature="timing")]
            let start = PreciseTime::now();

            let block = self
                .web3
                .eth()
                .block_with_txs(BlockId::from(next_block_number))
                .wait()?
                .unwrap();

            #[cfg(feature="timing")]
            {
                let end = PreciseTime::now();
                average_duration = average_duration + start.to(end);
            }

            trace!("Got block #{}", next_block_number);
            next_block_number += 1;
            processed += 1;

            write!(&mut sql_blocks, "{}\n", block.to_insert_values())?;

            trace!("Getting transactions for block");
            for tx in block.transactions.iter() {
                write!(
                    &mut data_transactions,
                    "{}\n",
                    tx.to_values(&self.operation)
                )?;
                processed_tx += 1;
            }
            trace!("Got {} transactions", processed_tx);
        }

        if processed == 0 {
            return Ok(0);
        }
        Self::trim_ends(&mut sql_blocks);

        let pg_tx = self.pg_client.transaction()?;
        // save the blocks
        #[cfg(feature="timing")]
        let start_blocks = PreciseTime::now();

        trace!("Storing {} blocks to DB using insert", processed);
        pg_tx.execute(&sql_blocks, &[])?;

        #[cfg(feature="timing")]
        let end_blocks = PreciseTime::now();

        #[cfg(feature="timing")]
        let start_tx = PreciseTime::now();

        if processed_tx > 0 {
            trace!("Storing {} transactions to DB using {}", processed_tx,
                match self.operation {
                    SqlOperation::Insert => "insert",
                    SqlOperation::Copy => "copy",
                }
            );

            match self.operation {
                SqlOperation::Insert => {
                    Self::trim_ends(&mut data_transactions);
                    pg_tx.execute(&data_transactions, &[])?;
                },
                SqlOperation::Copy => print!("{}", data_transactions),
            }
        }

        trace!("Commiting direct DB operations");
        pg_tx.commit()?;

        #[cfg(feature="timing")]
        let end_tx = PreciseTime::now();



        self.last_db_block = next_block_number - 1;
        info!(
            "Processed {} blocks. At {}/{}",
            processed, self.last_db_block, self.last_node_block
        );

        #[cfg(feature="timing")]
        info!(
            "Node get: {:.3}/{} DB blocks: {:.3}/{} DB tx: {:.3}/{}",
            average_duration,
            processed,
            start_blocks.to(end_blocks),
            processed,
            start_tx.to(end_tx),
            processed_tx
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
            self.last_db_block, self.last_node_block
        );

        match self.operation {
            SqlOperation::Insert => {},
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
