use std::time::{Duration, Instant};

use axum::{
    response::{IntoResponse, Response},
    routing::post,
    Json, Router,
};
use bytes::Bytes;
use control_spec::RedirectSpec;
use ed25519_dalek::PUBLIC_KEY_LENGTH;
use rand::{thread_rng, RngCore};
use tokio::time::sleep;

use crate::CLIENT;

async fn benchmark(Json(spec): Json<RedirectSpec>) -> Response {
    let mut block = vec![0; spec.block_size];
    thread_rng().fill_bytes(&mut block);
    let block = Bytes::from(block);

    let start = Instant::now();

    let (block_id, _, key) = async {
        anyhow::Ok(
            CLIENT
                .post(format!("{}/put", spec.put_url))
                .body(block.clone())
                .send()
                .await?
                .error_for_status()?
                .json::<(String, u64, [u8; PUBLIC_KEY_LENGTH])>()
                .await?,
        )
    }
    .await
    .expect("can put");

    while async {
        anyhow::Ok(
            CLIENT
                .get(format!("{}/put/{block_id}", spec.put_url))
                .send()
                .await?
                .error_for_status()?
                .json::<Option<Duration>>()
                .await?,
        )
    }
    .await
    .expect("can poll put")
    .is_none()
    {
        sleep(Duration::from_secs(1)).await
    }

    let put_latency = start.elapsed();

    sleep(Duration::from_secs(10)).await;

    async {
        loop {
            let response = CLIENT
                .post(format!("{}/get", spec.get_url))
                .query(&[("redirect", true)])
                .body(block.clone())
                .json(&(block_id.clone(), key))
                .send()
                .await?;
            if response.status() == axum::http::StatusCode::NOT_FOUND {
                sleep(Duration::from_secs(1)).await;
                continue;
            }
            response.error_for_status()?;
            break anyhow::Ok(());
        }
    }
    .await
    .expect("can get");
    let start = Instant::now();

    let mut get_block;
    while {
        get_block = async {
            anyhow::Ok(
                CLIENT
                    .get(format!("{}/get/{block_id}", spec.get_url))
                    .send()
                    .await?
                    .error_for_status()?
                    .bytes()
                    .await?,
            )
        }
        .await
        .expect("can poll get");
        get_block.is_empty()
    } {}

    let get_latency = start.elapsed();
    assert_eq!(get_block, block);

    Json((put_latency, get_latency)).into_response()
}

pub fn make_service() -> Router {
    Router::new().route("/", post(benchmark))
}
