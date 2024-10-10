use async_trait::async_trait;
use testcontainers::{
    core::{IntoContainerPort, WaitFor},
    runners::AsyncRunner,
    ContainerAsync, GenericImage, ImageExt,
};

#[async_trait]
pub trait TestDatabase: Send + Sync {
    /// Set up the test container using user-defined code.
    async fn setup<'a>(&'a mut self) -> anyhow::Result<()>;

    /// Retrieve the database connection URL after setup.
    fn get_db_url(&self) -> String;
}

pub struct PostgresTestDatabase {
    connection_string: String,
    postgres_container: Option<ContainerAsync<GenericImage>>,
}

impl PostgresTestDatabase {
    pub fn new() -> Self {
        PostgresTestDatabase {
            postgres_container: None,
            connection_string: String::new(),
        }
    }
}

#[async_trait]
impl TestDatabase for PostgresTestDatabase {
    /// Set up the Postgres container and get the database connection URL.
    async fn setup<'a>(&'a mut self) -> anyhow::Result<()> {
        self.postgres_container = Some(
            GenericImage::new("postgres", "14")
                .with_exposed_port(5432.tcp())
                .with_wait_for(WaitFor::message_on_stderr(
                    "database system is ready to accept connections",
                ))
                .with_env_var("POSTGRES_DB", "postgres")
                .with_env_var("POSTGRES_USER", "postgres")
                .with_env_var("POSTGRES_PASSWORD", "postgres")
                .start()
                .await
                .expect("Postgres started"),
        );

        // Retrieve the host and port of the container for the connection string
        let host = self
            .postgres_container
            .as_ref()
            .unwrap()
            .get_host()
            .await
            .expect("Failed to get host");

        let port = self
            .postgres_container
            .as_ref()
            .unwrap()
            .get_host_port_ipv4(5432)
            .await
            .expect("Failed to get port");

        // Create the Postgres connection string
        self.connection_string = format!("postgres://postgres:postgres@{}:{}/postgres", host, port);

        Ok(())
    }

    /// Retrieve the Postgres connection URL after the container has been set up.
    fn get_db_url(&self) -> String {
        self.connection_string.clone()
    }
}
