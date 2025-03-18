# Example Postgres events processor 

## About 

A basic processor that indexes events into Postgres. It uses the `process_function` utility function. 

## How to use
1. Install Postgres and Diesel CLI
2. Construct a `config.yaml` file. You can see `postgres-basic-events-example/example-config.yaml` as an example. 
3. cd ~/aptos-indexer-processors-sdk/example
4. cargo run -p postgres-basic-events-example -- -c postgres-basic-events-example/example-config.yaml
