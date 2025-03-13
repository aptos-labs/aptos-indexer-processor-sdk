-- Tracks latest processed version per processor
CREATE TABLE processor_status (
  processor VARCHAR(50) UNIQUE PRIMARY KEY NOT NULL,
  last_success_version BIGINT NOT NULL,
  last_updated TIMESTAMP NOT NULL DEFAULT NOW(),
  last_transaction_timestamp TIMESTAMP NULL
);

-- Tracks chain id
CREATE TABLE ledger_infos (chain_id BIGINT UNIQUE PRIMARY KEY NOT NULL);
