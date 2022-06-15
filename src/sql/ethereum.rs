extern crate web3;

use crate::sql::Sequelizable;
use web3::types::{Block, Transaction, H160, H256};

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
            "(DECODE('{:x}', 'hex'), {}, DECODE('{:x}', 'hex'), {}, {}, {}, {}, {}, {}, {}),",
                self.hash,
                self.nonce.as_u64(),
                self.block_hash.unwrap(),
                self.block_number.unwrap(),
                self.transaction_index.unwrap(),
                match self.from {
                    Some(src) => format!("DECODE('{:x}', 'hex')", src),
                    None => String::from("NULL"),
                },                
                match self.to {
                    Some(dest) => format!("DECODE('{:x}', 'hex')", dest),
                    None => String::from("NULL"),
                },
                self.value,
                self.gas,
                match self.gas_price {
                    Some(gp) => format!("{}", gp),
                    None => String::from("NULL"),
                },
            )
    }

    fn to_copy_values(&self) -> String {
        format!(
            "{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}",
            self.pg_hex::<H256>(self.hash),
            self.nonce.as_u64(),
            self.pg_hex::<H256>(self.block_hash.unwrap()),
            self.block_number.unwrap(),
            self.transaction_index.unwrap(),
            match self.from {
                Some(src) => self.pg_hex::<H160>(src),
                None => String::from("NULL"),
            },
            match self.to {
                Some(dest) => self.pg_hex::<H160>(dest),
                None => String::from("NULL"),
            },
            self.value,
            self.gas,
            match self.gas_price {
                Some(gp) => format!("{}", gp),
                None => String::from("NULL"),
            },        
        )
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
