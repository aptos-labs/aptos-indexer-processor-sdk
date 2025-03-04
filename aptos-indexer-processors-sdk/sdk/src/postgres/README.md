# Postgres crate

## About 
This crate provides a Postgres implementation for the integration layer between the Indexer SDK and Postgres. Features included are tracking the last processed version, retrieving the start version, and validating the chain id. The key components of this crate are core schema and models, Diesel utility functions, and trait implementations. 

## How to use
1. Install Postgres and Diesel CLI
2. Add the `aptos-indexer-processor-sdk` crate with the `postgres_full` feature in the `[dependencies]` section of your `Config.toml`:
```
aptos-indexer-processor-sdk = { git = "https://github.com/aptos-labs/aptos-indexer-processor-sdk.git", rev = "{COMMIT_HASH}", features = ["postgres_full"] }
```
3. Copy the `src/db` folder into where you are managing your Diesel migrations.