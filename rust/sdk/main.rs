use crate::{pipeline::Pipeline, stream::GrpcStream, timed_buffer::TimedBuffer};

fn main() {
    let stream = GrpcStream::new();
    let buffer = TimedBuffer::new();
    let pipeline = Pipeline::new();
    println!("Hello, world!");
}
