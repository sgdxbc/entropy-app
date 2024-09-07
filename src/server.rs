use axum::Router;
use ed25519_dalek::SigningKey;

use crate::{app, block::Parameters, broadcast, store::Store, NodeBook, NodeId};

#[derive(Debug)]
pub struct Config {
    pub local_id: NodeId,
    pub key: SigningKey,
    pub parameters: Parameters,
    pub nodes: NodeBook,
    pub num_block_packet: usize,
    pub mesh: Vec<String>,
}

pub fn make_service(config: Config, store: Store) -> (Router, app::Context, broadcast::Context) {
    let broadcast_config = broadcast::ContextConfig {
        local_id: config.local_id,
        mesh: config.mesh,
    };
    let (broadcast_router, broadcast_context, broadcast_api) =
        broadcast::make_service(broadcast_config);
    let app_service_config = app::ServiceConfig {
        local_id: config.local_id,
        key: config.key,
        parameters: config.parameters.clone(),
        num_node: config.nodes.len(),
    };
    let app_router = app::make_service(app_service_config, broadcast_api.invoke_sender);
    let app_context_config = app::ContextConfig {
        local_id: config.local_id,
        nodes: config.nodes,
        parameters: config.parameters,
        num_block_packet: config.num_block_packet,
    };
    let app_context = app::Context::new(app_context_config, store, broadcast_api.upcall);
    let router = Router::new()
        .nest("/entropy", app_router)
        .nest("/broadcast", broadcast_router);
    (router, app_context, broadcast_context)
}
