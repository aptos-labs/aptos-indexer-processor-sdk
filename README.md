# Aptos Indexer SDK

> [!WARNING]
> The Aptos Indexer SDK is experimental.
> If you're looking to build a production-grade processor, we recommend continuing to fork and build on top of the [aptos-indexer-processors](https://github.com/aptos-labs/aptos-indexer-processors) repo. However, if you're developing a new, experimental processor, you may start using the Aptos Indexer SDK today.

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

We’ve created a [Quickstart Guide to Aptos Indexer SDK](https://github.com/aptos-labs/aptos-indexer-processor-example) which gets you setup and running an events processor that indexes events on the Aptos blockchain. 

# Documentation

## Creating a step

To create a step in the SDK, implement these traits:

1. **Processable**
    
    ```rust
    #[async_trait]
    impl Processable for MyExtractorStep {
        type Input = Transaction;
        type Output = ExtractedDataModel;
        type RunType = AsyncRunType;
    
    		// Processes a batch of input items and returns a batch of output items.
        async fn process(
            &mut self,
            input: TransactionContext<Transaction>,
        ) -> Result<Option<TransactionContext<ExtractedDataModel>>, ProcessorError> {
            let extracted_data = ...
            // Extract data from items.data
            
            Ok(Some(TransactionContext {
                data: extracted_data,
                start_version: input.start_version,
                end_version: input.end_version,
                start_transaction_timestamp: input.start_transaction_timestamp,
                end_transaction_timestamp: input.end_transaction_timestamp,
                total_size_in_bytes: input.total_size_in_bytes,
            }))
        }
    }
    ```
    
2. **NamedStep**
    
    ```rust
    impl NamedStep for MyExtractorStep {
        fn name(&self) -> String {
            "MyExtractorStep".to_string()
        }
    }
    ```
    
3. Either **AsyncStep** or **PollableAsyncStep**, which defines how the step will be run in the processor.
    1. The most basic step is an `AsyncStep`, which processes a batch of input items and returns a batch of output items.  
        
        ```rust
        #[async_trait]
        impl Processable for MyExtractorStep {
            ...
            type RunType = AsyncRunType;
        		...
        }
        
        impl AsyncStep for MyExtractorStep {}
        ```
        
    2. A `PollableAsyncStep` does the same as `AsyncStep`, but it also periodically polls its internal state and returns a batch of output items if available.
        
        ```rust
        #[async_trait]
        impl<T> Processable for MyPollStep<T>
        where
            Self: Sized + Send + 'static,
            T: Send + 'static,
        {
            ...
            type RunType = PollableAsyncRunType;
        		...
        }
        
        #[async_trait]
        impl<T: Send + 'static> PollableAsyncStep for MyPollStep<T>
        where
            Self: Sized + Send + Sync + 'static,
            T: Send + 'static,
        {
        		/// Returns the duration between polls
            fn poll_interval(&self) -> std::time::Duration {
                // Define duration
            }
        
        		/// Polls the internal state and returns a batch of output items if available.
            async fn poll(&mut self) -> Result<Option<Vec<TransactionContext<T>>>, ProcessorError> {
                // Define polling logic
            }
        }
        ```
        

## Common steps

The SDK provides several common steps to use in your processor. 

1. `TransactionStreamStep` provides a stream of Aptos transactions to the processor
2. `TimedBufferStep` buffers a batch of items and periodically polls to release the items to the next step

## Connecting steps

When `ProcessorBuilder` connects two steps, a channel is created linking the two steps and the output of the first step becomes the input of the next step.

```rust
let (pb, buffer_receiver) = ProcessorBuilder::new_with_inputless_first_step(
      first_step.into_runnable_step(),
  )
  .connect_to(second_step.into_runnable_step(), channel_size)
  .connect_to(third_step.into_runnable_step(), channel_size)
  .end_and_return_output_receiver(channel_size);
```

## Adding a new processor

1. Use [aptos-indexer-processor-example](https://github.com/aptos-labs/aptos-indexer-processor-example) as a starting point
2. Add the new processor to [ProcessorConfig](https://github.com/aptos-labs/aptos-indexer-processor-example/blob/a8bbb23056d55b86b4ded6822c9120e5e8763d50/aptos-indexer-processor-example/src/config/processor_config.rs#L34) and [Processor](https://github.com/aptos-labs/aptos-indexer-processor-example/blob/a8bbb23056d55b86b4ded6822c9120e5e8763d50/aptos-indexer-processor-example/src/config/processor_config.rs#L58)
3. Add the processor to [RunnableConfig](https://github.com/aptos-labs/aptos-indexer-processor-example/blob/a8bbb23056d55b86b4ded6822c9120e5e8763d50/aptos-indexer-processor-example/src/config/indexer_processor_config.rs#L25)

## Running a processor

To run the processor, we recommend using the example in [aptos-indexer-processor-example](https://github.com/aptos-labs/aptos-indexer-processor-example) and following this [configuration guide](https://github.com/aptos-labs/aptos-indexer-processor-example?tab=readme-ov-file#configuring-your-processor).

## Advanced features (experimental)

1. Fanout + ArcifyStep
2. Fan in

