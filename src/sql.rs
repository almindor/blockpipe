mod ethereum;

use std::fmt::LowerHex;

pub const LAST_DB_BLOCK_QUERY: &str = "SELECT number FROM view_last_block";

#[derive(PartialEq)]
pub enum SqlOperation {
    Insert,
    Copy,
}

pub trait Sequelizable {
    fn table_name() -> &'static str;
    fn insert_fields() -> &'static str;

    fn to_insert_values(&self) -> String;
    fn to_copy_values(&self) -> String;

    fn to_values(&self, op: &SqlOperation) -> String {
        match op {
            SqlOperation::Insert => self.to_insert_values(),
            SqlOperation::Copy => self.to_copy_values(),
        }
    }

    fn pg_hex<HT: LowerHex>(&self, hash: HT) -> String {
        let mut result = String::with_capacity(32 * 2 + 3); // 2 hex chars per byte + \\x prefix
        let hex_raw = format!("{:x}", hash);

        // let mut even: bool = true;
        result += "\\\\x";
        for c in hex_raw.chars() {
            result.push(c);

            // even = !even;
        }

        result
    }
}
