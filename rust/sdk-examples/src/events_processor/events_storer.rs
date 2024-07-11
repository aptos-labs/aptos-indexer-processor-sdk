use super::events_models::EventModel;
use crate::{
    config::config::DbConfig,
    schema,
    utils::database::{execute_in_chunks, get_config_table_chunk_size, new_db_pool, PgDbPool},
};
use ahash::AHashMap;
use anyhow::{Context, Result};
use async_trait::async_trait;
use diesel::{
    pg::{upsert::excluded, Pg},
    query_builder::QueryFragment,
    ExpressionMethods,
};
use sdk::{
    steps::{async_step::AsyncRunType, AsyncStep},
    traits::{NamedStep, Processable},
    types::transaction_context::TransactionContext,
};

pub struct EventsStorer
where
    Self: Sized + Send + 'static,
{
    conn_pool: PgDbPool,
}

impl EventsStorer {
    pub async fn new(db_config: DbConfig) -> Result<Self> {
        let conn_pool = new_db_pool(
            &db_config.postgres_connection_string,
            Some(db_config.db_pool_size),
        )
        .await
        .context("Failed to create connection pool")?;
        Ok(Self { conn_pool })
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
    type RunType = AsyncRunType;

    async fn process(
        &mut self,
        events: TransactionContext<EventModel>,
    ) -> TransactionContext<EventModel> {
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
                // println!("Events stored successfully");
            },
            Err(e) => {
                println!("Failed to store events: {:?}", e);
            },
        }
        events
    }
}

impl AsyncStep for EventsStorer {}

impl NamedStep for EventsStorer {
    fn name(&self) -> String {
        "EventsStorer".to_string()
    }
}
