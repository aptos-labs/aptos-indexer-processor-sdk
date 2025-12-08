use super::basic_processor_step::BasicProcessorStep;
use crate::{
    aptos_indexer_transaction_stream::TransactionStreamConfig,
    builder::ProcessorBuilder,
    common_steps::{
        TransactionStreamStep, VersionTrackerStep, DEFAULT_UPDATE_PROCESSOR_STATUS_SECS,
    },
    postgres::{
        progress::PostgresProgressStatusProvider,
        subconfigs::postgres_config::PostgresConfig,
        utils::{
            checkpoint::{
                get_starting_version, PostgresChainIdChecker, PostgresProcessorStatusSaver,
            },
            database::{new_db_pool, run_migrations, ArcDbPool},
        },
        SDK_MIGRATIONS,
    },
    server_framework::{
        load, register_probes_and_metrics_handler, setup_logging, setup_panic_handler,
        GenericConfig, ProgressHealthChecker, ProgressHealthConfig, ReadinessCheck, ServerArgs,
    },
    traits::IntoRunnableStep,
    utils::{chain_id_check::check_or_update_chain_id, errors::ProcessorError},
};
use anyhow::Result;
use aptos_protos::transaction::v1::Transaction;
use clap::Parser;
use diesel_migrations::EmbeddedMigrations;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tracing::info;

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ProcessConfig {
    pub transaction_stream_config: TransactionStreamConfig,
    pub postgres_config: PostgresConfig,
    /// Optional configuration for progress health checking.
    /// If provided, the `/readiness` endpoint will check if the processor is making progress.
    #[serde(default)]
    pub progress_health_config: Option<ProgressHealthConfig>,
}

/// Processes transactions with a custom handler function.
pub async fn process<F, Fut>(
    processor_name: String,
    embedded_migrations: EmbeddedMigrations,
    process_function: F,
) -> Result<()>
where
    F: FnMut(Vec<Transaction>, ArcDbPool) -> Fut + Send + Sync + 'static,
    Fut: std::future::Future<Output = Result<(), ProcessorError>> + Send + 'static,
{
    let args = ServerArgs::parse();
    setup_logging();
    setup_panic_handler();
    let config = load::<GenericConfig<ProcessConfig>>(&args.config_path)?;
    let handle = tokio::runtime::Handle::current();

    let health_port = config.health_check_port;
    let additional_labels = config.metrics_config.additional_labels.clone();
    let progress_health_config = config.server_config.progress_health_config.clone();

    let db_pool = new_db_pool(
        &config.server_config.postgres_config.connection_string,
        Some(config.server_config.postgres_config.db_pool_size),
    )
    .await
    .expect("Failed to create connection pool");

    // Build readiness checks.
    let mut readiness_checks: Vec<Arc<dyn ReadinessCheck>> = vec![];
    if let Some(progress_config) = progress_health_config {
        let status_provider =
            PostgresProgressStatusProvider::new(processor_name.clone(), db_pool.clone());
        let progress_checker = ProgressHealthChecker::new(
            processor_name.clone(),
            Box::new(status_provider),
            progress_config,
        );
        readiness_checks.push(Arc::new(progress_checker));
    }

    // Start liveness and readiness probes.
    let task_handler = handle.spawn(async move {
        register_probes_and_metrics_handler(health_port, additional_labels, readiness_checks).await;
        anyhow::Ok(())
    });
    let main_task_handler = handle.spawn(async move {
        run_processor(
            processor_name,
            config.server_config.transaction_stream_config,
            config.server_config.postgres_config,
            embedded_migrations,
            db_pool,
            process_function,
        )
        .await
    });
    tokio::select! {
        res = task_handler => {
            res.expect("Probes and metrics handler unexpectedly exited")
        },
        res = main_task_handler => {
            res.expect("Main task handler unexpectedly exited")
        },
    }
}

pub async fn run_processor<F, Fut>(
    processor_name: String,
    transaction_stream_config: TransactionStreamConfig,
    postgres_config: PostgresConfig,
    embedded_migrations: EmbeddedMigrations,
    db_pool: ArcDbPool,
    process_function: F,
) -> Result<()>
where
    F: FnMut(Vec<Transaction>, ArcDbPool) -> Fut + Send + Sync + 'static,
    Fut: std::future::Future<Output = Result<(), ProcessorError>> + Send + 'static,
{
    // Run user migrations.
    run_migrations(
        postgres_config.connection_string.clone(),
        db_pool.clone(),
        embedded_migrations,
    )
    .await;

    // Run SDK migrations.
    run_migrations(
        postgres_config.connection_string.clone(),
        db_pool.clone(),
        SDK_MIGRATIONS,
    )
    .await;

    check_or_update_chain_id(
        &transaction_stream_config,
        &PostgresChainIdChecker::new(db_pool.clone()),
    )
    .await?;

    // Merge the starting version from config and the latest processed version from the DB.
    let starting_version = get_starting_version(
        processor_name.as_str(),
        transaction_stream_config.clone(),
        db_pool.clone(),
    )
    .await?;

    // Define processor steps.
    let transaction_stream_config = transaction_stream_config.clone();
    let transaction_stream = TransactionStreamStep::new(TransactionStreamConfig {
        starting_version: Some(starting_version),
        ..transaction_stream_config
    })
    .await?;
    let basic_processor_step = BasicProcessorStep {
        process_function,
        conn_pool: db_pool.clone(),
    };
    let processor_status_saver =
        PostgresProcessorStatusSaver::new(processor_name.as_str(), db_pool.clone());
    let version_tracker =
        VersionTrackerStep::new(processor_status_saver, DEFAULT_UPDATE_PROCESSOR_STATUS_SECS);

    // Connect processor steps together.
    let (_, buffer_receiver) =
        ProcessorBuilder::new_with_inputless_first_step(transaction_stream.into_runnable_step())
            .connect_to(basic_processor_step.into_runnable_step(), 10)
            .connect_to(version_tracker.into_runnable_step(), 10)
            .end_and_return_output_receiver(10);

    // (Optional) Parse the results.
    loop {
        match buffer_receiver.recv().await {
            Ok(_) => {},
            Err(_) => {
                info!("Channel is closed");
                return Ok(());
            },
        }
    }
}
