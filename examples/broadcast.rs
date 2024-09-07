use std::{collections::HashMap, net::SocketAddr, time::Duration};

use axum::Router;
use entropy_app::{broadcast, generate_nodes};
use rand::{seq::IteratorRandom, thread_rng};
use tokio::{
    net::TcpListener,
    sync::mpsc::unbounded_channel,
    task::JoinSet,
    time::{sleep, timeout},
};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // let n = 12;
    let n = 100;
    let mut rng = thread_rng();
    let addrs = (1..=n)
        .map(|i| SocketAddr::from(([127, 0, 0, i], 3000)))
        .collect();
    let (peers, _keys) = generate_nodes(addrs, &mut rng);
    let broadcast_configs = broadcast::ContextConfig::generate_network(&peers, 6, &mut rng)?;

    let mut sessions = JoinSet::new();
    let mut invokes = HashMap::new();
    let (monitor_sender, mut monitor_receiver) = unbounded_channel();
    for config in broadcast_configs {
        let id = config.local_id;
        let (router, context, mut api) = broadcast::make_service(config);

        let listener = TcpListener::bind(peers[&id].addr).await?;
        let app = Router::new().nest("/broadcast", router);
        sessions.spawn(async move {
            axum::serve(listener, app).await?;
            anyhow::Ok(())
        });
        sessions.spawn(context.session());
        let addr = peers[&id].addr;
        invokes.insert(addr, api.invoke_sender);
        let monitor_sender = monitor_sender.clone();
        sessions.spawn(async move {
            while let Some(result) = api.upcall.recv().await {
                anyhow::ensure!(result.is_empty());
                monitor_sender.send(addr)?
            }
            anyhow::Ok(())
        });
    }

    let app_session = async move {
        sleep(Duration::from_millis(100)).await;
        let addr = invokes.keys().choose(&mut rng).unwrap();
        println!("Send by {addr}");
        invokes[addr].send(Default::default())?;
        for i in 1..=n - 1 {
            println!("[{i:3}/{n:3}] Recv {:?}", monitor_receiver.recv().await);
        }
        println!("No more recv expected");
        println!(
            "{:?}",
            timeout(Duration::from_secs(1), monitor_receiver.recv()).await
        );
        anyhow::Ok(())
    };
    tokio::select! {
        result = app_session => return result,
        Some(result) = sessions.join_next() => result??,
    }
    anyhow::bail!("unreachable")
}
