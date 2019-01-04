extern crate web3;

use crate::sql::Sequelizable;
use web3::types::{Block, Transaction, H256, H160};

impl Sequelizable for Transaction {
    fn table_name() -> &'static str {
        "transactions"
    }

    fn insert_fields() -> &'static str {
        "hash, nonce, blockHash, blockNumber, transactionIndex, \
         \"from\", \"to\", \"value\", gas, gasPrice"
    }

    fn to_insert_values(&self) -> String {
        format!(
            "(DECODE('{:x}', 'hex'), {}, DECODE('{:x}', 'hex'), {}, {}, DECODE('{:x}', 'hex'), {}, {}, {}, {}),",
                self.hash,
                self.nonce.as_u64(),
                self.block_hash.unwrap(),
                self.block_number.unwrap(),
                self.transaction_index.unwrap(),
                self.from,
                match self.to {
                    Some(dest) => format!("DECODE('{:x}', 'hex')", dest),
                    None => String::from("NULL")
                },
                self.value,
                self.gas,
                self.gas_price)
    }

    fn to_copy_values(&self) -> String {
        format!(
            "{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}",
                self.pg_hex::<H256>(self.hash),
                self.nonce.as_u64(),
                self.pg_hex::<H256>(self.block_hash.unwrap()),
                self.block_number.unwrap(),
                self.transaction_index.unwrap(),
                self.pg_hex::<H160>(self.from),
                match self.to {
                    Some(dest) => self.pg_hex::<H160>(dest),
                    None => String::from("NULL")
                },
                self.value,
                self.gas,
                self.gas_price)
    }
}

impl<TX> Sequelizable for Block<TX> {
    fn table_name() -> &'static str {
        "blocks"
    }

    fn insert_fields() -> &'static str {
        "\"number\", hash, \"timestamp\""
    }

    fn to_insert_values(&self) -> String {
        format!(
            "({}, DECODE('{:x}', 'hex'), TO_TIMESTAMP({})),",
            self.number.unwrap().as_u64(),
            self.hash.unwrap(),
            self.timestamp.as_u64()
        )
    }

    fn to_copy_values(&self) -> String {
        format!(
            "{}\t{}\tTO_TIMESTAMP({})", // TODO: fix timestamp here, not used atm.
            self.number.unwrap().as_u64(),
            self.pg_hex::<H256>(self.hash.unwrap()),
            self.timestamp.as_u64()
        )
    }
}
