use std::sync::Arc;

use axum::Router;
use ed25519_dalek::SigningKey;
use tokio::sync::mpsc::unbounded_channel;

use crate::{
    app, block::Parameters, broadcast, glacier, redirect, replication, store::Store, NodeBook,
    NodeId,
};

#[derive(Debug)]
pub struct Config {
    pub local_id: NodeId,
    pub key: SigningKey,
    pub parameters: Parameters,
    pub nodes: Arc<NodeBook>,
    pub f: usize,
    pub node_bandwidth: usize,
    // entropy
    pub num_block_packet: usize,
    pub mesh: Vec<String>,
    // glacier
    pub ring_mesh: Vec<String>,
    pub group_size: usize,
    // replication
    pub regional_primaries: Vec<String>,
    pub regional_mesh: Vec<String>,
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
    replication::Context,
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
        node_bandwidth: config.node_bandwidth,
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
        nodes: config.nodes.clone(),
        local_id: config.local_id,
        parameters: config.parameters,
        node_bandwidth: config.node_bandwidth,
    };
    let glacier_context = glacier::Context::new(glacier_config, store.clone(), ring_api.upcall);

    let (block_sender, block_receiver) = unbounded_channel();
    let replication_service_config = replication::ServiceConfig {
        local_id: config.local_id,
        f: config.f,
        parameters: config.parameters,
        // very not good practice
        store_path: store.path.to_owned(),
        nodes: config.nodes.clone(),
    };
    let replication_router = replication::make_service(replication_service_config, block_sender);

    let replication_config = replication::ContextConfig {
        regional_primaries: config.regional_primaries,
        regional_mesh: config.regional_mesh,
    };
    let replication_context =
        replication::Context::new(replication_config, block_receiver, config.nodes);

    let redirect_router = redirect::make_service();

    let router = Router::new()
        .nest("/entropy", app_router)
        .nest("/broadcast", broadcast_router)
        .nest("/glacier", glacier_router)
        .nest("/ring", ring_router)
        .nest("/replication", replication_router)
        .nest("/redirect", redirect_router);
    (
        router,
        app_context,
        broadcast_context,
        glacier_context,
        ring_context,
        replication_context,
    )
}
