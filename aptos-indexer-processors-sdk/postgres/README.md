# Postgres crate

## About 
This crate provides a Postgres implementation for the integration layer between the Indexer SDK and Postgres. Features included are tracking the last processed version, backfilling, retrieving the start version, and validating the chain id. The key components of this crate are core schema and models, Diesel utility functions, and trait implementations. 

## How to use
1. Install Postgres and Diesel CLI
2. Add the crate to your `Cargo.toml` by including the following lines under the `[dependencies]` section:
```
aptos-indexer-processor-sdk-postgres = { git = "https://github.com/aptos-labs/aptos-indexer-processor-sdk.git", rev = "{COMMIT_HASH}" }
```
3. Copy the `src/db` folder into where you are managing your Diesel migrations.