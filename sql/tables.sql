CREATE ROLE bp_writer WITH LOGIN;
ALTER ROLE bp_writer WITH PASSWORD 'password';
CREATE ROLE bp_reader WITH LOGIN;
ALTER ROLE bp_reader WITH PASSWORD 'password';

CREATE DOMAIN ADDRESS AS BYTEA CHECK(length(value) = 20);
CREATE DOMAIN H256 AS BYTEA CHECK(length(value) = 32);
CREATE DOMAIN U256 AS NUMERIC;

CREATE UNLOGGED TABLE blocks (
  "number" BIGINT PRIMARY KEY, -- don't use numeric here since it's unsupported in rust. i64 is good enough until 2100+
  hash H256 NOT NULL UNIQUE,
  "timestamp" TIMESTAMP NOT NULL
);

CREATE UNLOGGED TABLE transactions (
  hash H256 PRIMARY KEY,
  nonce U256,
  blockHash H256 NOT NULL REFERENCES blocks(hash) ON DELETE CASCADE ON UPDATE CASCADE,
  blockNumber BIGINT NOT NULL REFERENCES blocks("number") ON DELETE CASCADE ON UPDATE CASCADE,
  transactionIndex U256 NOT NULL,
  "from" ADDRESS NOT NULL,
  "to" ADDRESS,
  "value" U256 NOT NULL,
  gas U256 NOT NULL,
  gasPrice U256 NOT NULL
);

CREATE VIEW view_last_block
AS
SELECT b.number
FROM blocks b
WHERE b.number = (SELECT MAX(b2.number) FROM blocks b2);

CREATE INDEX idx_transactions_from
ON transactions("from");

CREATE INDEX idx_transactions_to
ON transactions("to");

CREATE VIEW view_blocks
AS SELECT b.number, ENCODE(b.hash, 'hex') AS hash, b.hash AS hash_raw, b.timestamp
FROM blocks b;

CREATE VIEW view_transactions
AS SELECT hash AS hash_raw, ENCODE(hash, 'hex') AS hash, blockNumber, ENCODE(blockHash, 'hex') AS blockHash, ENCODE(t.from, 'hex') AS "from", t.from AS from_raw, ENCODE(t.to, 'hex') AS "to", t.to AS to_raw, t.value, gas, gasPrice
FROM transactions t;

GRANT SELECT ON TABLE view_last_block TO bp_writer;
GRANT INSERT ON TABLE blocks TO bp_writer;
GRANT INSERT, SELECT, UPDATE ON TABLE transactions TO bp_writer;

GRANT SELECT ON TABLE view_blocks TO bp_reader;
GRANT SELECT ON TABLE view_transactions TO bp_reader;
