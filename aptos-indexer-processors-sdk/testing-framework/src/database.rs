use anyhow::{Context, Result};
use async_trait::async_trait;
use testcontainers::{
    core::{IntoContainerPort, WaitFor},
    runners::AsyncRunner,
    ContainerAsync, GenericImage, ImageExt,
};

const POSTGRES_IMAGE: &str = "postgres";
const POSTGRES_VERSION: &str = "14";
const POSTGRES_PORT: u16 = 5432;
const POSTGRES_DB: &str = "postgres";
const POSTGRES_USER: &str = "postgres";
const POSTGRES_PASSWORD: &str = "postgres";

#[async_trait]
pub trait TestDatabase: Send + Sync {
    /// Set up the test container using user-defined code.
    async fn setup<'a>(&'a mut self) -> anyhow::Result<()>;

    /// Retrieve the database connection URL after setup.
    fn get_db_url(&self) -> String;
}

#[derive(Default)]
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

    /// Helper method to configure and start the Postgres container.
    async fn start_postgres_container(&mut self) -> Result<ContainerAsync<GenericImage>> {
        let postgres_image = GenericImage::new(POSTGRES_IMAGE, POSTGRES_VERSION)
            .with_exposed_port(POSTGRES_PORT.tcp())
            .with_wait_for(WaitFor::message_on_stderr(
                "database system is ready to accept connections",
            ))
            .with_env_var("POSTGRES_DB", POSTGRES_DB)
            .with_env_var("POSTGRES_USER", POSTGRES_USER)
            .with_env_var("POSTGRES_PASSWORD", POSTGRES_PASSWORD);

        let container = postgres_image
            .start()
            .await
            .context("Failed to start Postgres container")?;

        Ok(container)
    }

    /// Helper method to get the host and port information of the running container.
    async fn get_connection_info(&self) -> Result<(String, u16)> {
        let host = self
            .postgres_container
            .as_ref()
            .context("Postgres container not initialized")?
            .get_host()
            .await
            .context("Failed to get container host")?;

        let port = self
            .postgres_container
            .as_ref()
            .context("Postgres container not initialized")?
            .get_host_port_ipv4(5432)
            .await
            .context("Failed to get container port")?;

        Ok((host.to_string(), port))
    }
}

#[async_trait]
impl TestDatabase for PostgresTestDatabase {
    /// Set up the Postgres container and get the database connection URL.
    async fn setup(&mut self) -> Result<()> {
        self.postgres_container = Some(self.start_postgres_container().await?);

        let (host, port) = self.get_connection_info().await?;

        self.connection_string = format!("postgres://postgres:postgres@{}:{}/postgres", host, port);
        Ok(())
    }

    /// Retrieve the Postgres connection URL after the container has been set up.
    fn get_db_url(&self) -> String {
        self.connection_string.clone()
    }
}
