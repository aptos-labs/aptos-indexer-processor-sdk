use crate::{
    traits::{AsyncRunType, AsyncStep, NamedStep, Processable},
    types::transaction_context::TransactionContext,
    utils::{
        errors::ProcessorError,
        step_metrics::{StepMetricLabels, WRITE_RATE_LIMIT_STEP_REMAINING_BYTES},
    },
};
use serde::{Deserialize, Serialize};
use std::{
    marker::PhantomData,
    time::{Duration, Instant},
};

/// Config for WriteRateLimitStep. For example: num_bytes=10,000,000, num_seconds=300
/// means that the processor can write up to 10 MB per 5 min bucket.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct WriteRateLimitConfig {
    /// This is the maximum number of bytes that can be written in the given number of seconds.
    pub num_bytes: u64,
    /// This is the number of seconds over which the `num_bytes` limit is applied.
    pub num_seconds: u64,
}

/// This step limits the number of bytes that can be written to the DB per second, based
/// on a config specifying the number of bytes that can be written in a given number of
/// seconds. See `WriteRateLimitConfig` for more.
///
/// This uses "leaky bucket" semantics. The bucket starts off full (`num_bytes`). When
/// an item is allowed to pass to the next step, we remove tokens from the bucket
/// (`current_bucket_size`) based on the size of the item. As time passes, we refill the
/// bucket as per `fill_rate` (which is derived from `WriteRateLimitConfig`) up until
/// `max_bucket_size`. If we get an item larger than the current number of tokens in the
/// bucket, we sleep until we know there will be enough tokens to write the item.
///
/// We mandate that the Input implement Sizeable so we can calculate its size. The dev
/// can implement this whichever way works best for them, e.g. making their struct
/// derive allocative::Allocative or get_size::GetSize and then using the result from
/// the related functions, or just implementing it by hand.
///
/// This should go before the DB writing step.
pub struct WriteRateLimitStep<Input>
where
    Self: Sized + Send + 'static,
    Input: Send + Sizeable + 'static,
{
    /// This is the maximum number of bytes that can be written in the given number of
    /// seconds (aka capacity). This is just `num_bytes` directly from the config.
    max_bucket_size: f64,
    /// This is the current number of bytes that can be written this instant (aka
    /// tokens). The bucket starts out full.
    current_bucket_size: f64,
    /// This is the rate at which bytes are added to the bucket per second. This is
    /// derived from `WriteRateLimitConfig`, `num_bytes` / `num_seconds`.
    fill_rate: f64,
    /// This is the last time the bucket was updated.
    last_updated: Instant,
    phantom: PhantomData<Input>,
}

impl<Input> WriteRateLimitStep<Input>
where
    Self: Sized + Send + 'static,
    Input: Send + Sizeable + 'static,
{
    pub fn new(config: WriteRateLimitConfig) -> Self {
        let capacity = config.num_bytes as f64;
        let fill_rate = capacity / config.num_seconds as f64;
        Self {
            max_bucket_size: capacity,
            fill_rate,
            // Start with a full bucket.
            current_bucket_size: capacity,
            last_updated: Instant::now(),
            phantom: PhantomData,
        }
    }

    // Helper function to update tokens based on time elapsed since the last update.
    fn update_tokens(&mut self) {
        let now = Instant::now();
        let elapsed = now.duration_since(self.last_updated).as_secs_f64();
        self.current_bucket_size =
            (self.current_bucket_size + elapsed * self.fill_rate).min(self.max_bucket_size);
        self.last_updated = now;
    }
}

#[async_trait::async_trait]
impl<Input> Processable for WriteRateLimitStep<Input>
where
    Self: Sized + Send + 'static,
    Input: Send + Sizeable + 'static,
{
    type Input = Input;
    type Output = Input;
    type RunType = AsyncRunType;

    async fn process(
        &mut self,
        item: TransactionContext<Input>,
    ) -> Result<Option<TransactionContext<Input>>, ProcessorError> {
        let size_of_item = Sizeable::size_in_bytes(&item.data) as f64;

        self.update_tokens();

        let out = if self.current_bucket_size >= size_of_item {
            // We have enough tokens, proceed.
            Some(item)
        } else {
            // Not enough tokens, calculate how long we have to wait until we have enough.
            let tokens_needed = size_of_item - self.current_bucket_size;
            let wait_time = tokens_needed / self.fill_rate;

            // Wait until enough tokens are available.
            tokio::time::sleep(Duration::from_secs_f64(wait_time)).await;

            // Now that we've waited, refill the tokens. We should have enough for the
            // size of the items now.
            self.update_tokens();

            Some(item)
        };

        // Deduct the size of the items from the tokens bucket.
        self.current_bucket_size -= size_of_item;

        // Push metrics indicating how many bytes we have remaining. If this value is
        // at / close to zero, we know we're hitting the write ratelimit.
        WRITE_RATE_LIMIT_STEP_REMAINING_BYTES
            .get_or_create(&StepMetricLabels {
                step_name: self.name(),
            })
            .set(self.current_bucket_size as i64);

        Ok(out)
    }
}

impl<Input: Send + Sizeable + 'static> AsyncStep for WriteRateLimitStep<Input> {}

impl<Input: Send + Sizeable + 'static> NamedStep for WriteRateLimitStep<Input> {
    fn name(&self) -> String {
        format!("WriteRateLimitStep: {}", std::any::type_name::<Input>())
    }
}

/// To use an item with `WriteRateLimitStep`, it must implement `Sizeable`. The intent
/// of `WriteRateLimitStep` is to put an upper bound on the write load on the DB, so the
/// implementation of this trait should reflect the size of the data in terms of what it
/// would use in the DB (assuming a insertion), not necessarily the size of the data in
/// memory. You can implement this in whatever way works best for you, e.g. to reflect
/// size of data on disk vs write load (aiming to constrain iops).
pub trait Sizeable {
    /// Get the size of the item in bytes. See the trait documentation for more info.
    fn size_in_bytes(&self) -> u64;
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        builder::ProcessorBuilder,
        test::{steps::pass_through_step::PassThroughStep, utils::receive_with_timeout},
        traits::{IntoRunnableStep, RunnableStepWithInputReceiver},
        types::transaction_context::TransactionMetadata,
    };
    use instrumented_channel::instrumented_bounded_channel;
    use std::time::Duration;

    #[derive(Clone, Debug, PartialEq)]
    struct TestData {
        content: Vec<u8>,
    }

    impl Sizeable for TestData {
        /// This is just a dummy implementation, in practice you'd use something like
        /// the `get_size` crate.
        fn size_in_bytes(&self) -> u64 {
            // We include some overhead for the size of the vec, just as an example.
            self.content.len() as u64 + 24
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_write_rate_limit_step() {
        // Create a WriteRateLimitConfig with known parameters.
        let num_bytes = 1000;
        let num_seconds = 1;
        let config = WriteRateLimitConfig {
            num_bytes,
            num_seconds,
        };

        let write_rate_limit_step = WriteRateLimitStep::<TestData>::new(config);
        let step_metric_labels = StepMetricLabels {
            step_name: write_rate_limit_step.name(),
        };

        // Create an input channel.
        let (input_sender, input_receiver) = instrumented_bounded_channel("input", 10);
        let input_step = RunnableStepWithInputReceiver::new(
            input_receiver,
            PassThroughStep::default().into_runnable_step(),
        );

        // Build the processor.
        let (_pb, mut output_receiver) =
            ProcessorBuilder::new_with_runnable_input_receiver_first_step(input_step)
                .connect_to(write_rate_limit_step.into_runnable_step(), 5)
                .end_and_return_output_receiver(5);

        // Now, create some TestData items. Each item is 800 bytes.
        let item_size = 800;
        let data1 = TestData {
            content: vec![0u8; item_size],
        };
        let data2 = TestData {
            content: vec![0u8; item_size],
        };

        // Measure the size of the data using the same function that the step uses.
        let data1_size = Sizeable::size_in_bytes(&data1);

        let transaction_context1 = TransactionContext {
            data: data1.clone(),
            metadata: TransactionMetadata {
                start_version: 0,
                end_version: 0,
                start_transaction_timestamp: None,
                end_transaction_timestamp: None,
                total_size_in_bytes: item_size as u64,
            },
        };

        let transaction_context2 = TransactionContext {
            data: data2.clone(),
            metadata: TransactionMetadata {
                start_version: 0,
                end_version: 0,
                start_transaction_timestamp: None,
                end_transaction_timestamp: None,
                total_size_in_bytes: item_size as u64,
            },
        };

        // Record the start time.
        let start_time = tokio::time::Instant::now();

        // Send the first item.
        input_sender.send(transaction_context1).await.unwrap();

        // Receive the first output.
        let result1 = receive_with_timeout(&mut output_receiver, 1000)
            .await
            .unwrap();
        let elapsed1 = tokio::time::Instant::now().duration_since(start_time);

        // Confirm that the remaining bytes metric was updated correctly. It should
        // equal the initial capacity minus the size of the first item at this point.
        // Note that the data will be a bit larger than 800 bytes due to the size of the
        // vec itself (see `sizeable::size_of`) and `data1_size` above.
        let remaining_bytes = WRITE_RATE_LIMIT_STEP_REMAINING_BYTES
            .get_or_create(&step_metric_labels)
            .get();
        let expected = (num_bytes - data1_size) as i64;
        assert_eq!(remaining_bytes, expected);

        // Send the second item.
        input_sender.send(transaction_context2).await.unwrap();

        // Receive the second output.
        let result2 = receive_with_timeout(&mut output_receiver, 2000)
            .await
            .unwrap();
        let elapsed2 = tokio::time::Instant::now().duration_since(start_time);

        // Assert that the first item was processed immediately.
        assert!(
            elapsed1 < Duration::from_millis(100),
            "First item was not processed immediately."
        );

        // Assert that the second item was delayed by approximately 0.6 seconds.
        let time_diff = elapsed2 - elapsed1;

        assert!(
            time_diff >= Duration::from_millis(500),
            "Expected at least 500ms delay between processing, got {:?}",
            time_diff
        );

        assert!(
            time_diff <= Duration::from_millis(700),
            "Expected less than 700ms delay between processing, got {:?}",
            time_diff
        );

        // Ensure the outputs are as expected.
        assert_eq!(result1.data, data1);
        assert_eq!(result2.data, data2);

        // Confirm that the remaining bytes metric was updated correctly. It should be
        // close to zero at this point, but it'll be a little over 0 since the tokens
        // will have refilled a bit.
        let remaining_bytes = WRITE_RATE_LIMIT_STEP_REMAINING_BYTES
            .get_or_create(&step_metric_labels)
            .get();
        assert!(remaining_bytes < 100);
    }
}
