/**

# Instrumented Channel
This is a wrapper and abstraction over the kanal channel (for now), but it can be extended to support other channels as well.

The main purpose of this crate is to provide a way to instrument the channel, so that we can track the number of messages sent and received, and the time taken to send and receive messages.

## Example
```rust
use instrumented_channel::instrumented_channel;
use tokio::time::{sleep, Duration};

#[tokio::main]
async fn main() {
    let (sender, receiver) = instrumented_channel(10);
    let send = sender.send(42);
    let recv = receiver.recv();
    tokio::spawn(async move {
        sleep(Duration::from_secs(1)).await;
        drop(send);
    });
    tokio::select! {
        _ = send => println!("send completed"),
        _ = recv => println!("recv completed"),
    }
}
```

 **/




