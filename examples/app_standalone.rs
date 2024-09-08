use std::{collections::HashMap, env::temp_dir, net::SocketAddr, time::Duration};

use ed25519_dalek::PUBLIC_KEY_LENGTH;
use entropy_app::{
    block::Parameters,
    broadcast, generate_nodes,
    node::{build, Config},
    store::Store,
};
use rand::thread_rng;
use tokio::{fs::create_dir_all, net::TcpListener, task::JoinSet, time::sleep};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let f = 33;
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

    let activity_session = async move {
        sleep(Duration::from_secs(1)).await;
        let client = reqwest::Client::new();
        for i in 1..=10 {
            println!("start");
            let (block_id, checksum, verifying_key) = client
                .post("http://127.0.0.1:3000/entropy/put")
                .send()
                .await?
                .error_for_status()?
                .json::<(String, u64, [u8; PUBLIC_KEY_LENGTH])>()
                .await?;
            println!("put {block_id} checksum {checksum:08x}");
            loop {
                if let Some(latency) = client
                    .get(format!("http://127.0.0.1:3000/entropy/put/{block_id}"))
                    .send()
                    .await?
                    .error_for_status()?
                    .json::<Option<Duration>>()
                    .await?
                {
                    println!("{latency:?}");
                    break;
                }
            }

            sleep(Duration::from_secs(1)).await;
            println!("get {block_id}");
            client
                .post(format!("http://127.0.0.{i}:3000/entropy/get"))
                .json(&(block_id.clone(), verifying_key))
                .send()
                .await?
                .error_for_status()?;
            loop {
                if let Some((latency, other_checksum)) = client
                    .get(format!("http://127.0.0.{i}:3000/entropy/get/{block_id}"))
                    .send()
                    .await?
                    .error_for_status()?
                    .json::<Option<(Duration, u64)>>()
                    .await?
                {
                    println!("{latency:?} checksum {other_checksum:08x}");
                    anyhow::ensure!(other_checksum == checksum);
                    break;
                }
            }
            println!("wait for verifying post activity system stability");
            sleep(Duration::from_secs(1)).await;
        }
        anyhow::Ok(())
    };

    tokio::select! {
        result = activity_session => return result,
        Some(result) = sessions.join_next() => result??,
    }
    anyhow::bail!("unreachable")
}
