#[cfg(target_os = "linux")]
use aptos_system_utils::profiling::start_cpu_profiling;

#[cfg(target_os = "linux")]
use std::convert::Infallible;

use anyhow::Context;
use prometheus::{Encoder, TextEncoder};
use warp::Filter;
use warp::http::Response;


/// Register readiness and liveness probes and set up metrics endpoint
async fn register_probes_and_metrics_handler(port: u16) {
    let readiness = warp::path("readiness")
        .map(move || warp::reply::with_status("ready", warp::http::StatusCode::OK));
    let metrics_endpoint = warp::path("metrics").map(|| {
        // Metrics encoding.
        let metrics = prometheus::gather();
        let mut encode_buffer = vec![];
        let encoder = TextEncoder::new();
        // If metrics encoding fails, we want to panic and crash the process.
        encoder
            .encode(&metrics, &mut encode_buffer)
            .context("Failed to encode metrics")
            .unwrap();

        Response::builder()
            .header("Content-Type", "text/plain")
            .body(encode_buffer)
    });

    if cfg!(target_os = "linux") {
        #[cfg(target_os = "linux")]
            let profilez = warp::path("profilez").and_then(|| async move {
            // TODO(grao): Consider make the parameters configurable.
            Ok::<_, Infallible>(match start_cpu_profiling(10, 99, false).await {
                Ok(body) => {
                    let response = Response::builder()
                        .header("Content-Length", body.len())
                        .header("Content-Disposition", "inline")
                        .header("Content-Type", "image/svg+xml")
                        .body(body);

                    match response {
                        Ok(res) => warp::reply::with_status(res, warp::http::StatusCode::OK),
                        Err(e) => warp::reply::with_status(
                            Response::new(format!("Profiling failed: {e:?}.").as_bytes().to_vec()),
                            warp::http::StatusCode::INTERNAL_SERVER_ERROR,
                        ),
                    }
                }
                Err(e) => warp::reply::with_status(
                    Response::new(format!("Profiling failed: {e:?}.").as_bytes().to_vec()),
                    warp::http::StatusCode::INTERNAL_SERVER_ERROR,
                ),
            })
        });
        #[cfg(target_os = "linux")]
        warp::serve(readiness.or(metrics_endpoint).or(profilez))
            .run(([0, 0, 0, 0], port))
            .await;
    } else {
        warp::serve(readiness.or(metrics_endpoint))
            .run(([0, 0, 0, 0], port))
            .await;
    }
}