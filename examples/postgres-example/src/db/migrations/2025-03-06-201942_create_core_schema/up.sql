-- Your SQL goes here
-- Tracks latest processed version per processor
CREATE TABLE processor_status (
  processor VARCHAR(50) UNIQUE PRIMARY KEY NOT NULL,
  last_success_version BIGINT NOT NULL,
  last_updated TIMESTAMP NOT NULL DEFAULT NOW(),
  last_transaction_timestamp TIMESTAMP NULL
);

-- Tracks backfill progress per processor
CREATE TABLE backfill_processor_status (
    backfill_alias VARCHAR(50) NOT NULL,
    backfill_status VARCHAR(50) NOT NULL,
    last_success_version BIGINT NOT NULL,
    last_updated TIMESTAMP NOT NULL DEFAULT NOW(),
    last_transaction_timestamp TIMESTAMP NULL,
    backfill_start_version BIGINT NOT NULL,
    backfill_end_version BIGINT NOT NULL,
    PRIMARY KEY (backfill_alias)
);

-- Tracks chain id
CREATE TABLE ledger_infos (chain_id BIGINT UNIQUE PRIMARY KEY NOT NULL);