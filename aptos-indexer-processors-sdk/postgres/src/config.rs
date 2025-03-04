use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct PostgresConfig {
    pub connection_string: String,
    // Size of the pool for writes/reads to the DB. Limits maximum number of queries in flight
    #[serde(default = "PostgresConfig::default_db_pool_size")]
    pub db_pool_size: u32,
}

impl PostgresConfig {
    pub const fn default_db_pool_size() -> u32 {
        150
    }
}
