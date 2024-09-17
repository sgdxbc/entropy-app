use axum::{
    response::{IntoResponse, Response},
    routing::post,
    Json, Router,
};
use bytes::Bytes;
use rand::{thread_rng, RngCore};
use serde::Deserialize;

#[derive(Deserialize)]
struct BenchmarkSpec {
    put_url: String,
    get_url: String,
    block_size: usize,
}

async fn benchmark(Json(spec): Json<BenchmarkSpec>) -> Response {
    let mut block = vec![0; spec.block_size];
    thread_rng().fill_bytes(&mut block);
    let block = Bytes::from(block);
    ().into_response()
}

pub fn make_service() -> Router {
    Router::new().route("/", post(benchmark))
}
