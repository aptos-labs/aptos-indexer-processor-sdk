use crate::{
    db::common::models::events_models::EventModel,
    schema,
    utils::database::{execute_in_chunks, get_config_table_chunk_size, ArcDbPool},
};
use ahash::AHashMap;
use anyhow::Result;
use aptos_indexer_processor_sdk::{
    traits::{
        async_step::AsyncRunType, processable::CustomRunType, AsyncStep, IntoRunnableStep,
        NamedStep, Processable, RunnableAsyncStep, RunnableStep,
    },
    types::transaction_context::TransactionContext,
    utils::{
        errors::ProcessorError,
        step_metrics::{StepMetricLabels, StepMetricsBuilder},
    },
};
use async_trait::async_trait;
use bigdecimal::Zero;
use diesel::{
    pg::{upsert::excluded, Pg},
    query_builder::QueryFragment,
    ExpressionMethods,
};
use instrumented_channel::{
    instrumented_bounded_channel, InstrumentedAsyncReceiver, InstrumentedAsyncSender,
};
use std::time::{Duration, Instant};
use tokio::task::JoinHandle;
use tracing::{error, info, warn};

pub struct EventsStorer
where
    Self: Sized + Send + 'static,
{
    conn_pool: ArcDbPool,
}

impl EventsStorer {
    pub fn new(conn_pool: ArcDbPool) -> Self {
        Self { conn_pool }
    }
}

fn insert_events_query(
    items_to_insert: Vec<EventModel>,
) -> (
    impl QueryFragment<Pg> + diesel::query_builder::QueryId + Send,
    Option<&'static str>,
) {
    use schema::events::dsl::*;
    (
        diesel::insert_into(schema::events::table)
            .values(items_to_insert)
            .on_conflict((transaction_version, event_index))
            .do_update()
            .set((
                inserted_at.eq(excluded(inserted_at)),
                indexed_type.eq(excluded(indexed_type)),
            )),
        None,
    )
}

#[async_trait]
impl Processable for EventsStorer {
    type Input = EventModel;
    type Output = EventModel;
    type RunType = CustomRunType;

    async fn process(
        &mut self,
        events: TransactionContext<EventModel>,
    ) -> Result<Option<TransactionContext<EventModel>>, ProcessorError> {
        let per_table_chunk_sizes: AHashMap<String, usize> = AHashMap::new();
        let execute_res = execute_in_chunks(
            self.conn_pool.clone(),
            insert_events_query,
            &events.data,
            get_config_table_chunk_size::<EventModel>("events", &per_table_chunk_sizes),
        )
        .await;
        match execute_res {
            Ok(_) => {
                info!(
                    "Events version [{}, {}] stored successfully",
                    events.start_version, events.end_version
                );
            },
            Err(e) => {
                error!("Failed to store events: {:?}", e);
            },
        }
        Ok(Some(events))
    }
}

impl AsyncStep for EventsStorer {}

impl NamedStep for EventsStorer {
    fn name(&self) -> String {
        "EventsStorer".to_string()
    }
}

// This trait implementation is required if you want to customize running the step
impl IntoRunnableStep<EventModel, EventModel, EventsStorer, CustomRunType> for EventsStorer {
    fn into_runnable_step(self) -> impl RunnableStep<EventModel, EventModel> {
        RunnableEventsStorer::new(self)
    }
}

pub struct RunnableEventsStorer {
    pub step: EventsStorer,
}

impl RunnableEventsStorer {
    pub fn new(step: EventsStorer) -> Self {
        Self { step }
    }
}

impl RunnableStep<EventModel, EventModel> for RunnableEventsStorer {
    fn spawn(
        self,
        input_receiver: Option<InstrumentedAsyncReceiver<TransactionContext<EventModel>>>,
        output_channel_size: usize,
        _input_sender: Option<InstrumentedAsyncSender<TransactionContext<EventModel>>>,
    ) -> (
        InstrumentedAsyncReceiver<TransactionContext<EventModel>>,
        JoinHandle<()>,
    ) {
        let mut step = self.step;
        let step_name = step.name();
        let input_receiver = input_receiver.expect("Input receiver must be set");

        let (output_sender, output_receiver) =
            instrumented_bounded_channel(&step_name, output_channel_size);

        // TIP: You may replace this tokio task with your own code to customize the parallelization of this step
        info!(step_name = step_name, "Spawning processing task");
        let handle = tokio::spawn(async move {
            loop {
                let input_with_context = match input_receiver.recv().await {
                    Ok(input_with_context) => input_with_context,
                    Err(e) => {
                        // If the previous steps have finished and the channels have closed , we should break out of the loop
                        warn!(
                            step_name = step_name,
                            error = e.to_string(),
                            "No input received from channel"
                        );
                        break;
                    },
                };
                let processing_duration = Instant::now();
                let output_with_context = match step.process(input_with_context).await {
                    Ok(output_with_context) => output_with_context,
                    Err(e) => {
                        error!(
                            step_name = step_name,
                            error = e.to_string(),
                            "Failed to process input"
                        );
                        break;
                    },
                };
                if let Some(output_with_context) = output_with_context {
                    match StepMetricsBuilder::default()
                        .labels(StepMetricLabels {
                            step_name: step.name(),
                        })
                        .latest_processed_version(output_with_context.end_version)
                        .latest_transaction_timestamp(
                            output_with_context.get_start_transaction_timestamp_unix(),
                        )
                        .num_transactions_processed_count(
                            output_with_context.get_num_transactions(),
                        )
                        .processing_duration_in_secs(processing_duration.elapsed().as_secs_f64())
                        .processed_size_in_bytes(output_with_context.total_size_in_bytes)
                        .build()
                    {
                        Ok(mut metrics) => metrics.log_metrics(),
                        Err(e) => {
                            error!(
                                step_name = step_name,
                                error = e.to_string(),
                                "Failed to log metrics"
                            );
                            break;
                        },
                    }
                    match output_sender.send(output_with_context).await {
                        Ok(_) => (),
                        Err(e) => {
                            error!(
                                step_name = step_name,
                                error = e.to_string(),
                                "Error sending output to channel"
                            );
                            break;
                        },
                    }
                }
            }

            // Wait for output channel to be empty before ending the task and closing the send channel
            loop {
                let channel_size = output_sender.len();
                info!(
                    step_name = step_name,
                    channel_size = channel_size,
                    "Waiting for output channel to be empty"
                );
                if channel_size.is_zero() {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
            info!(
                step_name = step_name,
                "Output channel is empty. Closing send channel."
            );
        });

        (output_receiver, handle)
    }
}

impl NamedStep for RunnableEventsStorer {
    fn name(&self) -> String {
        self.step.name()
    }

    fn type_name(&self) -> String {
        let step_type = std::any::type_name::<EventsStorer>().to_string();
        format!("{} (via RunnableAsyncStep)", step_type)
    }
}
