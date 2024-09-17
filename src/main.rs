use std::{
    collections::HashMap,
    env::{args, temp_dir},
    net::SocketAddr,
    sync::Arc,
};

use axum::routing::get;
use control_spec::SystemSpec;
use entropy_app::{
    block::Parameters,
    broadcast, generate_nodes, glacier,
    node::{build, Config},
    store::Store,
};
use rand::{rngs::StdRng, SeedableRng};
use tokio::{
    fs::{create_dir_all, read},
    net::TcpListener,
};

#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

#[tokio::main]
// #[tokio::main(flavor = "current_thread")]
async fn main() -> anyhow::Result<()> {
    let rlimit = nix::sys::resource::getrlimit(nix::sys::resource::Resource::RLIMIT_NOFILE)?;
    nix::sys::resource::setrlimit(
        nix::sys::resource::Resource::RLIMIT_NOFILE,
        rlimit.1,
        rlimit.1,
    )?;

    let spec = serde_json::from_slice::<SystemSpec>(&read("./spec.json").await?)?;
    let addrs = serde_json::from_slice::<Vec<(SocketAddr, Option<String>, String)>>(
        &read("./addrs.json").await?,
    )?[..spec.n]
        .to_vec();
    let index = args()
        .nth(1)
        .ok_or(anyhow::format_err!("missing index argument"))?
        .parse::<usize>()?;
    let (addr, region, url) = addrs[index].clone();

    let parameters = Parameters {
        chunk_size: spec.chunk_size,
        k: spec.k,
    };

    let mut rng = StdRng::seed_from_u64(117418);
    let (nodes, node_keys) =
        generate_nodes(addrs.iter().map(|(addr, _, _)| addr).copied(), &mut rng);
    let network = broadcast::ContextConfig::generate_network(&nodes, spec.degree, &mut rng)?
        .into_iter()
        .map(|config| (config.local_id, config.mesh))
        .collect::<HashMap<_, _>>();
    let nodes = Arc::new(nodes);
    let ring = glacier::ring::ContextConfig::construct_ring(
        nodes.clone(),
        spec.group_size, //
    )?
    .into_iter()
    .map(|config| (config.local_id, config.mesh))
    .collect::<HashMap<_, _>>();
    let mut region_addrs = HashMap::<_, Vec<_>>::new();
    for (_, region, url) in addrs {
        region_addrs.entry(region).or_default().push(url)
    }
    let mut regional_primaries = region_addrs
        .values_mut()
        .map(|addrs| addrs.remove(0))
        .collect::<Vec<_>>();
    let regional_mesh = if let Some(pos) = regional_primaries
        .iter()
        .position(|other_url| *other_url == url)
    {
        regional_primaries.remove(pos);
        region_addrs.remove(&region).expect("has region")
    } else {
        Default::default()
    };

    let (&node_id, _) = nodes
        .iter()
        .find(|(_, node)| node.addr == addr)
        .expect("can find node addr");
    let config = Config {
        local_id: node_id,
        key: node_keys[&node_id].clone(),
        parameters,
        nodes: nodes.clone(),
        num_block_packet: spec.num_block_packet,
        mesh: network[&node_id].clone(),
        f: spec.f,
        node_bandwidth: spec.node_bandwidth,
        ring_mesh: ring[&node_id].clone(),
        group_size: spec.group_size,
        regional_primaries,
        regional_mesh,
    };
    let store_dir = temp_dir().join("entropy").join(format!("{node_id:?}"));
    create_dir_all(&store_dir).await?;
    let store = Store::new(store_dir);
    let (
        router,
        app_context,
        broadcast_context,
        glacier_context,
        ring_context,
        replication_context,
    ) = build(config, store);
    let router = router.route("/ok", get(|| async {}));

    let mut addr = nodes[&node_id].addr;
    addr.set_ip([0; 4].into());
    println!("listen {addr}");
    let listener = TcpListener::bind(addr).await?;
    let endpoint = async move {
        axum::serve(listener, router).await?;
        anyhow::Ok(())
    };
    tokio::select! {
        result = endpoint => result?,
        result = app_context.session() => result?,
        result = broadcast_context.session() => result?,
        result = glacier_context.session() => result?,
        result = ring_context.session() => result?,
        result = replication_context.session() => result?,
    }
    anyhow::bail!("unreachable")
}
