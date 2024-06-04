use tracing_subscriber::EnvFilter;

/// Set up logging
/// TODO: maybe use `set_global_logger` from the `aptos_logger` crate??
pub fn setup_logging() {
    let env_filter = EnvFilter::try_from_default_env()
        .or_else(|_| EnvFilter::try_new("info"))
        .unwrap();

    tracing_subscriber::fmt()
        .json()
        .with_file(true)
        .with_line_number(true)
        .with_thread_ids(true)
        .with_target(false)
        .with_thread_names(true)
        .with_env_filter(env_filter)
        .init();
}
