use crate::{
    steps::{
        async_step::AsyncRunType, event_extractor::EventModel,
        pollable_async_step::PollableAsyncRunType, AsyncStep, PollableAsyncStep,
    },
    traits::{NamedStep, Processable},
};
use anyhow::{Context, Result};
use aptos_indexer_transaction_stream::TransactionsPBResponse;
use aptos_protos::transaction::v1::transaction::TxnData;
use async_trait::async_trait;
use bevy_reflect::Reflect;
use rayon::prelude::*;
use serde::{Deserialize, Serialize};
use std::{path::PathBuf, time::Duration};
use tokio::{
    io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader},
    net::UnixStream,
};
use tracing;

// needs to be configurable as the end or not, so it just sends or sends and receives.
// for now it just sends on an arbitrary serde json Value.

#[derive(Clone, Debug, Deserialize, Reflect, Serialize)]
pub struct UnixSocketLambdaConfig {
    // TODO: ofc use something more strongly typed.
    socket_path: PathBuf,
}

/**
 * Dispatch items to whatever is listening on the other end of a unix socket.
 *
 *
 */
pub struct UnixSocketLambda
where
    Self: Sized + Send + 'static,
{
    // todo handle reconnects
    stream: UnixStream,
}

impl UnixSocketLambda
where
    Self: Sized + Send + 'static,
{
    pub async fn new(config: UnixSocketLambdaConfig) -> Result<Self> {
        // Establish unix socket connection.
        let stream = UnixStream::connect(config.socket_path.clone())
            .await
            .with_context(|| {
                format!(
                    "Failed to connect to unix socket at {}",
                    config.socket_path.display()
                )
            })?;
        Ok(Self { stream })
    }
}

// You
#[async_trait]
impl Processable for UnixSocketLambda
where
    Self: Sized + Send + 'static,
{
    // todo don't hardcode
    type Input = EventModel;
    type Output = serde_json::Value;
    // TODO: It is possible to use AsyncRunType here. If you do, impling PollableAsyncStep
    // does nothing, and you won't be able to call into_runnable_step. It'd be nice if this
    // problem was more obvious / harder to run into.
    type RunType = PollableAsyncRunType;

    // todo remove the newline stuff, this is just for the demo
    // send items through the unix socket.
    async fn process(&mut self, items: Vec<EventModel>) -> Vec<Self::Output> {
        let num_items = items.len();
        println!("[{}]: Received {} items", self.name(), num_items);

        let items = items
            .into_iter()
            .map(|item| {
                let mut bytes = serde_json::to_vec(&item).expect("Failed to serialize item");
                bytes.push(b'\n'); // Append newline character
                bytes
            })
            .collect::<Vec<Vec<u8>>>();

        for mut bytes in items {
            while !bytes.is_empty() {
                match self.stream.write(&bytes).await {
                    Ok(0) => break, // Should not happen in a non-blocking context, but just in case
                    Ok(n) => bytes = bytes[n..].to_vec(), // Remove written bytes from the buffer
                    Err(e) => {
                        eprintln!("Failed to write to socket: {:?}", e);
                        break;
                    }
                }
            }
        }

        println!("[{}]: Sent {} items", self.name(), num_items);
        Vec::new()
    }
}

#[async_trait]
impl PollableAsyncStep for UnixSocketLambda
where
    Self: Sized + Send + 'static,
{
    // TODO: I think if poll hangs, it hangs everything: https://www.notion.so/aptoslabs/Processor-SDK-Design-Review-Notes-b1b832b880ee45cabf59672fdf72bdae?pvs=4#15f2dfb019784b4a89e17b757d8d7216
    // Receive items from the unix socket, send them onward. One JSON item per line.
    async fn poll(&mut self) -> Option<Vec<serde_json::Value>> {
        println!("Polling unix socket");
        let name = self.name();

        let mut reader = BufReader::new(&mut self.stream);
        let mut results = Vec::new();

        loop {
            let mut line = String::new();

            let read_future = reader.read_line(&mut line);

            match tokio::time::timeout(Duration::from_secs(1), read_future).await {
                Ok(Ok(0)) => break, // EOF reached, break the loop
                Ok(Ok(_)) => {
                    line = line.trim().to_string(); // Remove any trailing newline characters
                    if line.is_empty() {
                        continue;
                    }
                    match serde_json::from_str(&line) {
                        Ok(item) => {
                            println!("[{}]: Sending 1 item", name);
                            results.push(item);
                        },
                        Err(e) => {
                            println!("Error deserializing item: {:?}", e);
                        },
                    }
                },
                Ok(Err(e)) => {
                    println!("Error reading line from socket: {:?}", e);
                    break;
                }
                Err(_) => {
                    // Timeout reached, no data available to read
                    println!("Polling timed out, no data available");
                    break;
                }
            }
        }

        println!("[{}] Received {} items", name, results.len());

        if results.is_empty() {
            None
        } else {
            Some(results)
        }
    }

    fn poll_interval(&self) -> Option<Duration> {
        Some(Duration::from_secs(1))
    }
}

impl NamedStep for UnixSocketLambda {
    fn name(&self) -> String {
        "UnixSocketLambda".to_string()
    }
}
