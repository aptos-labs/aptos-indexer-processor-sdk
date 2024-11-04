use crate::{
    traits::{
        pollable_async_step::PollableAsyncRunType, NamedStep, PollableAsyncStep, Processable,
    },
    types::transaction_context::TransactionContext,
    utils::errors::ProcessorError,
};
use anyhow::Result;
use async_trait::async_trait;
use google_cloud_storage::{
    client::Client as GCSClient,
    http::objects::upload::{Media, UploadObjectRequest, UploadType},
};
use std::time::Duration;

pub struct TableConfig {
    pub table_name: String,
    pub bucket_name: String,
    pub max_size: usize,
}

pub struct TimedSizeBufferStep<Input>
where
    Input: Send + 'static + Sized,
{
    pub internal_buffer: Vec<TransactionContext<Input>>,
    pub poll_interval: Duration,
    pub table_config: TableConfig,
    pub gcs_client: GCSClient,
}

impl<Input> TimedSizeBufferStep<Input>
where
    Input: Send + 'static + Sized,
{
    pub fn new(poll_interval: Duration, table_config: TableConfig, gcs_client: GCSClient) -> Self {
        Self {
            internal_buffer: Vec::new(),
            poll_interval,
            table_config,
            gcs_client,
        }
    }

    async fn upload_to_gcs(&self) -> Result<(), ProcessorError> {
        let parquet_data = self.convert_to_parquet(&self.internal_buffer)?;

        let object_name = format!(
            "{}/data_{}.parquet",
            self.table_config.table_name,
            chrono::Utc::now().timestamp()
        );

        // self.gcs_client.upload_object(&self.table_config.bucket_name, &object_name, parquet_data).await?;

        Ok(())
    }

    // Conversion logic specific to each table based on `table_config`
    fn convert_to_parquet(
        &self,
        data: &[TransactionContext<Input>],
    ) -> Result<Vec<u8>, ProcessorError> {
        Ok(vec![])
    }
}

#[async_trait]
impl<Input> Processable for TimedSizeBufferStep<Input>
where
    Input: Send + Sync + 'static + Sized,
{
    type Input = Input;
    type Output = Input;
    type RunType = PollableAsyncRunType;

    async fn process(
        &mut self,
        item: TransactionContext<Input>,
    ) -> Result<Option<TransactionContext<Input>>, ProcessorError> {
        self.internal_buffer.push(item);

        if self.internal_buffer.len() >= self.table_config.max_size {
            self.upload_to_gcs().await?;
            // return Ok(Some(std::mem::take(&mut self.internal_buffer)));
            return Ok(None);
        }

        Ok(None)
    }

    async fn cleanup(
        &mut self,
    ) -> Result<Option<Vec<TransactionContext<Self::Output>>>, ProcessorError> {
        if !self.internal_buffer.is_empty() {
            self.upload_to_gcs().await?;
        }
        // Ok(Some(std::mem::take(&mut self.internal_buffer)))
        Ok(None)
    }
}

#[async_trait]
impl<Input: Send + Sync + 'static> PollableAsyncStep for TimedSizeBufferStep<Input> {
    fn poll_interval(&self) -> Duration {
        self.poll_interval
    }

    async fn poll(&mut self) -> Result<Option<Vec<TransactionContext<Input>>>, ProcessorError> {
        if !self.internal_buffer.is_empty() {
            self.upload_to_gcs().await?;
            return Ok(Some(std::mem::take(&mut self.internal_buffer)));
        }
        Ok(None)
    }
}

impl<Input: Send + 'static> NamedStep for TimedSizeBufferStep<Input> {
    fn name(&self) -> String {
        format!("TimedSizeBuffer: {}", self.table_config.table_name)
    }
}
