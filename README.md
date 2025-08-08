# Aptos Indexer SDK
Generally, an indexer processor follow this flow:

1. Receive a stream of Aptos transactions
2. Extract data from the transactions
3. Transform and merge the parsed data into a coherent, standardized schema
4. Store the transformed data into a database

The Aptos Indexer SDK works by modeling each processor as a graph of independent steps. Each of the steps in the flow above is written as a `Step` in the SDK, and the output of each `Step` is connected to the input of the next `Step` by a channel.

# How to use

To your `Cargo.toml` , add

```yaml
aptos-indexer-processor-sdk = { git = "https://github.com/aptos-labs/aptos-indexer-processor-sdk.git", rev = "{COMMIT_HASH}" }
aptos-indexer-processor-sdk-server-framework = { git = "https://github.com/aptos-labs/aptos-indexer-processor-sdk.git", rev = "{COMMIT_HASH}" }
```

# Get started

We’ve created a [Quickstart Guide to Aptos Indexer SDK](https://aptos.dev/build/indexer/indexer-sdk/quickstart) which gets you setup and running an events processor that indexes events on the Aptos blockchain. 

# Documentation
Full documentation can be found [here](https://aptos.dev/build/indexer/indexer-sdk/documentation)

