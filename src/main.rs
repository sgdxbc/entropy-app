use std::{
    collections::HashMap,
    env::{args, temp_dir},
    net::SocketAddr,
};

use axum::routing::get;
use control_spec::SystemSpec;
use entropy_app::{
    block::Parameters,
    broadcast, generate_nodes,
    node::{build, Config},
    store::Store,
};
use rand::{rngs::StdRng, SeedableRng};
use tokio::{
    fs::{create_dir_all, read},
    net::TcpListener,
};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let addrs = serde_json::from_slice::<Vec<SocketAddr>>(&read("./addrs.json").await?)?;
    let spec = serde_json::from_slice::<SystemSpec>(&read("./spec.json").await?)?;
    let index = args()
        .nth(1)
        .ok_or(anyhow::format_err!("missing index argument"))?
        .parse::<usize>()?;
    let parameters = Parameters {
        chunk_size: spec.chunk_size,
        k: (spec.n - spec.f * 2) * spec.num_block_packet,
    };

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
        num_block_packet: spec.num_block_packet,
        mesh: network[&node_id].clone(),
    };
    let store_dir = temp_dir().join("entropy").join(format!("{node_id:?}"));
    create_dir_all(&store_dir).await?;
    let store = Store::new(store_dir);
    let (router, app_context, broadcast_context) = build(config, store);
    let router = router.route("/ok", get(|| async {}));

    let mut addr = nodes[&node_id].addr;
    addr.set_ip([0; 4].into());
    let listener = TcpListener::bind(addr).await?;
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
