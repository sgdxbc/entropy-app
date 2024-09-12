use std::sync::Arc;

use axum::Router;
use ed25519_dalek::SigningKey;

use crate::{app, block::Parameters, broadcast, glacier, store::Store, NodeBook, NodeId};

#[derive(Debug)]
pub struct Config {
    pub local_id: NodeId,
    pub key: SigningKey,
    pub parameters: Parameters,
    pub nodes: Arc<NodeBook>,
    pub f: usize,
    // entropy
    pub num_block_packet: usize,
    pub mesh: Vec<String>,
    // glacier
    pub ring_mesh: Vec<String>,
    pub group_size: usize,
}

pub fn build(
    config: Config,
    store: Store,
) -> (
    Router,
    app::Context,
    broadcast::Context,
    glacier::Context,
    glacier::ring::Context,
) {
    let store = Arc::new(store);

    let broadcast_config = broadcast::ContextConfig {
        local_id: config.local_id,
        mesh: config.mesh,
    };
    let (broadcast_router, broadcast_context, broadcast_api) =
        broadcast::make_service(broadcast_config);

    let app_service_config = app::ServiceConfig {
        local_id: config.local_id,
        key: config.key.clone(),
        parameters: config.parameters,
        f: config.f,
    };
    let app_router = app::make_service(app_service_config, broadcast_api.invoke_sender);

    let app_context_config = app::ContextConfig {
        local_id: config.local_id,
        nodes: config.nodes.clone(),
        parameters: config.parameters,
        num_block_packet: config.num_block_packet,
    };
    let app_context = app::Context::new(app_context_config, store.clone(), broadcast_api.upcall);

    let ring_config = glacier::ring::ContextConfig {
        local_id: config.local_id,
        mesh: config.ring_mesh,
        nodes: config.nodes.clone(),
    };
    let (ring_router, ring_context, ring_api) = glacier::ring::make_service(ring_config);

    let glacier_service_config = glacier::ServiceConfig {
        local_id: config.local_id,
        key: config.key,
        parameters: config.parameters,
        f: config.f,
        n: config.group_size,
    };
    let glacier_router = glacier::make_service(glacier_service_config, ring_api.invoke_sender);

    let glacier_config = glacier::ContextConfig {
        nodes: config.nodes,
        local_id: config.local_id,
        parameters: config.parameters,
    };
    let glacier_context = glacier::Context::new(glacier_config, store, ring_api.upcall);

    let router = Router::new()
        .nest("/entropy", app_router)
        .nest("/broadcast", broadcast_router)
        .nest("/glacier", glacier_router)
        .nest("/ring", ring_router);
    (
        router,
        app_context,
        broadcast_context,
        glacier_context,
        ring_context,
    )
}
