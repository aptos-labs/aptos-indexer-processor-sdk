CREATE SCHEMA IF NOT EXISTS processor_metadata;

-- Tracks latest processed version per processor
CREATE TABLE IF NOT EXISTS processor_metadata.processor_status (
  processor VARCHAR(100) UNIQUE PRIMARY KEY NOT NULL,
  last_success_version BIGINT NOT NULL,
  last_updated TIMESTAMP NOT NULL DEFAULT NOW(),
  last_transaction_timestamp TIMESTAMP NULL
);

-- Tracks chain id
CREATE TABLE IF NOT EXISTS processor_metadata.ledger_infos (chain_id BIGINT UNIQUE PRIMARY KEY NOT NULL);
