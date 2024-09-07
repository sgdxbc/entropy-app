use std::{collections::HashMap, env::temp_dir, net::SocketAddr};

use entropy_app::{
    block::Parameters,
    broadcast, generate_nodes,
    server::{build, Config},
    store::Store,
};
use rand::thread_rng;
use tokio::{fs::create_dir_all, net::TcpListener, task::JoinSet};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let f = 3;
    let num_block_packet = 10;
    let parameters = Parameters {
        chunk_size: 1 << 10,
        k: (f + 1) * num_block_packet,
    };

    let mut rng = thread_rng();
    let addrs = (1..=3 * f + 1)
        .map(|i| SocketAddr::from(([127, 0, 0, i as _], 3000)))
        .collect();
    let (nodes, node_keys) = generate_nodes(addrs, &mut rng);
    let network = broadcast::ContextConfig::generate_network(&nodes, 6, &mut rng)?
        .into_iter()
        .map(|config| (config.local_id, config.mesh))
        .collect::<HashMap<_, _>>();

    let mut sessions = JoinSet::new();
    for (node_id, key) in node_keys {
        let config = Config {
            local_id: node_id,
            key,
            parameters: parameters.clone(),
            nodes: nodes.clone(),
            num_block_packet,
            mesh: network[&node_id].clone(),
        };
        let store_dir = temp_dir().join("entropy").join(format!("{node_id:?}"));
        create_dir_all(&store_dir).await?;
        let store = Store::new(store_dir);
        let (router, app_context, broadcast_context) = build(config, store);

        let listener = TcpListener::bind(nodes[&node_id].addr).await?;
        sessions.spawn(async move {
            axum::serve(listener, router).await?;
            anyhow::Ok(())
        });
        sessions.spawn(app_context.session());
        sessions.spawn(broadcast_context.session());
    }
    if let Some(result) = sessions.join_next().await {
        result??
    }
    anyhow::bail!("unreachable")
}
