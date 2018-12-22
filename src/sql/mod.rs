mod ethereum;

pub const LAST_DB_BLOCK_QUERY: &str = "SELECT number FROM view_last_block";

pub trait Sequelizable {
    fn table_name() -> &'static str;
    fn insert_fields() -> &'static str;

    fn to_insert_values(&self) -> String;
}
