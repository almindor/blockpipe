use std::time;
use tokio_postgres::{Client, NoTls};

use std::fmt::Write;
use std::string::String;

use crate::sql;
use crate::sql::{Sequelizable, SqlOperation};
use web3::types::{Block, BlockId, SyncState, Transaction, U64};
use web3::Transport;
use web3::Web3;

use log::{info, trace};
use simple_signal::{self, Signal};

mod error;

const MAX_BLOCKS_PER_BATCH: i32 = 100;

#[allow(dead_code)]
pub struct Pipe<T: Transport> {
    web3: Web3<T>,
    pg_client: Client,
    last_db_block: u64, // due to BIGINT and lack of NUMERIC support in driver
    last_node_block: u64,
    syncing: bool,
    operation: SqlOperation,
    sleep_r: crossbeam_channel::Receiver<i32>,
    running: bool,
}

impl<T: Transport> Pipe<T> {
    const ONE_MINUTE: time::Duration = time::Duration::from_secs(60);

    // we want to return error here if db fails
    #[allow(clippy::new_ret_no_self)]
    pub async fn new(
        transport: T,
        pg_path: &str,
        op: SqlOperation,
        last_block_override: i64,
        sleep_r: crossbeam_channel::Receiver<i32>,
    ) -> Result<Pipe<T>, Box<dyn std::error::Error>> {
        let (pg_client, connection) = tokio_postgres::connect(pg_path, NoTls).await?;

        tokio::spawn(async move {
            if let Err(e) = connection.await {
                panic!("PG connection error: {}", e);
            }
        });

        let rows = pg_client.query(sql::LAST_DB_BLOCK_QUERY, &[]).await?;
        let mut last_db_block_number = match rows.get(0) {
            Some(row) => row.get(0),
            None => 0,
        };

        if last_block_override > last_db_block_number {
            last_db_block_number = last_block_override;
        }

        let web3 = Web3::new(transport);

        Ok(Pipe {
            web3,
            pg_client,
            last_db_block: last_db_block_number as u64,
            last_node_block: 0,
            syncing: false,
            operation: op,
            running: true,
            sleep_r,
        })
    }

    async fn update_node_info(&mut self) -> Result<bool, web3::Error> {
        info!("Getting info from eth node.");
        let last_block_number = self.web3.eth().block_number().await?.as_u64();
        let syncing = self.web3.eth().syncing().await?;

        self.last_node_block = last_block_number;
        self.syncing = syncing != SyncState::NotSyncing;
        Ok(true)
    }

    fn sleep_with_msg(&mut self, msg: &str) {
        info!("{}", msg);
        // if it's ok means we got a channel send from the singal handler
        if self.sleep_r.recv_timeout(Self::ONE_MINUTE).is_ok() {
            log::warn!("sleep interrupted");
            self.running = false;
        }
    }

    fn sleep_when_syncing(&mut self) -> bool {
        if self.syncing {
            self.sleep_with_msg("Node is syncing, sleeping for a minute.");
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

    async fn store_next_batch(&mut self) -> Result<i32, error::PipeError> {
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
            && self.running
        {
            #[cfg(feature = "timing")]
            let start = PreciseTime::now();

            trace!("Getting block #{}", next_block_number);
            let block = self
                .web3
                .eth()
                .block_with_txs(BlockId::from(U64::from(next_block_number)))
                .await
                .unwrap()
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

        let pg_tx = self.pg_client.transaction().await?;
        // save the blocks

        trace!("Storing {} blocks to DB using insert", processed);
        pg_tx.execute(sql_blocks.as_str(), &[]).await?;

        if processed_tx > 0 {
            match self.operation {
                SqlOperation::Insert => {
                    trace!("Storing {} transactions to DB using insert", processed_tx);
                    // upsert in case of reorg
                    Self::trim_ends(&mut data_transactions);
                    write!(&mut data_transactions, "\nON CONFLICT (hash) DO UPDATE SET nonce = excluded.nonce, blockHash = excluded.blockHash, blockNumber = excluded.blockNumber, transactionIndex = excluded.transactionIndex, \"from\" = excluded.from, \"to\" = excluded.to, \"value\" = excluded.value, gas = excluded.gas, gasPrice = excluded.gasPrice")?;
                    pg_tx.execute(data_transactions.as_str(), &[]).await?;
                    trace!("Commiting direct DB operations");
                    pg_tx.commit().await?;
                }
                SqlOperation::Copy => {
                    trace!("Commiting direct DB operations");
                    pg_tx.commit().await?;
                    trace!("Storing {} transactions to DB using copy", processed_tx);
                    print!("{}", data_transactions);
                }
            }
        } else {
            trace!("Commiting direct DB operations");
            pg_tx.commit().await?;
        }

        self.last_db_block = next_block_number - 1;
        info!(
            "Processed {} blocks. At {}/{}",
            processed, self.last_db_block, self.last_node_block
        );

        Ok(processed)
    }

    pub async fn main(&mut self) -> Result<i32, error::PipeError> {
        self.update_node_info().await?;
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

        while self.last_db_block < self.last_node_block && self.running {
            self.store_next_batch().await?;
        }

        if self.running {
            self.sleep_with_msg("Run done, sleeping for one minute.");
        }

        Ok(0)
    }

    pub async fn run(
        &mut self,
        sleep_s: crossbeam_channel::Sender<i32>,
    ) -> Result<i32, error::PipeError> {
        simple_signal::set_handler(&[Signal::Int, Signal::Term], move |_signals| {
            info!("Exiting...");
            sleep_s.send(1).unwrap();
        });

        let mut iteration: u64 = 0;
        while self.running {
            iteration += 1;
            trace!("Main loop iteration #{}", iteration);
            self.main().await?;
        }

        Ok(0)
    }
}
