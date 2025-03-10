# Traits

## Async Step

The `async_step.rs` file provides tools for handling asynchronous steps in processing. 

Implement `AsyncStep` for steps that process data directly without buffering. 

## Pollable Async Step

The `pollable_async_step.rs` file provides tools for handling steps that can be polled asynchronously.

Implement `PollableAsyncStep` for steps that buffer or poll data over a duration of time in an asynchronous manner.

## Processable
The `processable.rs` file defines the `Processable` trait, which each step implements.

## Processor trait 
The `processor_trait.rs` defines `ProcessorTrait`, which each processor implements. 

