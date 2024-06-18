use super::events_models::EventModel;
use crate::{
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
};

pub struct EventsStorer
where
    Self: Sized + Send + 'static,
{
    conn_pool: PgDbPool,
}

impl EventsStorer {
    pub async fn new(
        postgres_connection_string: String,
        db_pool_size: Option<u32>,
    ) -> Result<Self> {
        let conn_pool = new_db_pool(&postgres_connection_string, db_pool_size)
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

    async fn process(&mut self, events: Vec<EventModel>) -> Vec<EventModel> {
        let per_table_chunk_sizes: AHashMap<String, usize> = AHashMap::new();
        let execute_res = execute_in_chunks(
            self.conn_pool.clone(),
            insert_events_query,
            &events,
            get_config_table_chunk_size::<EventModel>("events", &per_table_chunk_sizes),
        )
        .await;
        match execute_res {
            Ok(_) => {
                println!("Events stored successfully");
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
