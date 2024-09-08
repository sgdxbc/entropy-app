use std::{collections::HashMap, env::temp_dir, net::SocketAddr};

use entropy_app::{
    block::Parameters,
    broadcast, generate_nodes,
    node::{build, Config},
    store::Store,
};
use rand::{rngs::StdRng, SeedableRng};
use tokio::{fs::create_dir_all, net::TcpListener};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let f = 33;
    let num_block_packet = 10;
    let parameters = Parameters {
        chunk_size: 1 << 10,
        k: (f + 1) * num_block_packet,
    };
    let addrs = (1..=3 * f + 1)
        .map(|i| SocketAddr::from(([127, 0, 0, i as _], 3000)))
        .collect();
    let index = 0;

    let mut rng = StdRng::seed_from_u64(117418);
    let (nodes, node_keys) = generate_nodes(addrs, &mut rng);
    let network = broadcast::ContextConfig::generate_network(&nodes, 6, &mut rng)?
        .into_iter()
        .map(|config| (config.local_id, config.mesh))
        .collect::<HashMap<_, _>>();

    let mut node_ids = nodes.keys().copied().collect::<Vec<_>>();
    node_ids.sort_unstable();
    let node_id = node_ids
        .get(index)
        .copied()
        .ok_or(anyhow::format_err!("node index out of bound"))?;
    let config = Config {
        local_id: node_id,
        key: node_keys[&node_id].clone(),
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
    let endpoint = async move {
        axum::serve(listener, router).await?;
        anyhow::Ok(())
    };
    tokio::select! {
        result = endpoint => result?,
        result = app_context.session() => result?,
        result = broadcast_context.session() => result?,
    }
    anyhow::bail!("unreachable")
}
