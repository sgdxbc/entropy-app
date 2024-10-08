use std::{
    env::args,
    fmt::Write,
    future::Future,
    pin::pin,
    sync::{
        atomic::{AtomicUsize, Ordering::SeqCst},
        Arc, LazyLock,
    },
    time::{Duration, UNIX_EPOCH},
};

use control::{join_all, reload, terraform_output, Node};
use control_spec::{
    Protocol::{self, Entropy, Glacier, Replication},
    RedirectSpec, SystemSpec,
};
use rand::{
    seq::{IteratorRandom, SliceRandom},
    thread_rng,
};
use reqwest::StatusCode;
use tokio::{
    fs::write,
    task::JoinSet,
    time::{sleep, Instant},
};

const PUBLIC_KEY_LENGTH: usize = 32;

#[tokio::main(flavor = "current_thread")]
async fn main() -> anyhow::Result<()> {
    fn entropy(f: usize) -> SystemSpec {
        let n = 3 * f + 1;
        let num_block_packet = 10;
        SystemSpec {
            n,
            f,
            node_bandwidth: (10 << 30) / 8,
            protocol: Entropy,
            chunk_size: 32 << 10,
            // chunk_size: 1 << 10,
            k: ((n - 2 * f) * num_block_packet).min(32 << 10),
            num_block_packet,
            degree: 6,
            group_size: 0,
        }
    }

    fn glacier(n: usize, f: usize) -> SystemSpec {
        let group_size = 3 * f + 1;
        SystemSpec {
            n,
            f,
            node_bandwidth: (10 << 30) / 8,
            protocol: Glacier,
            chunk_size: 32 << 20,
            // chunk_size: 1 << 10,
            k: (group_size - 2 * f).min(32),
            num_block_packet: 0,
            degree: 0,
            group_size,
        }
    }

    fn replication(f: usize) -> SystemSpec {
        SystemSpec {
            n: 3 * f + 1,
            f,
            node_bandwidth: (10 << 30) / 8,
            protocol: Replication,
            chunk_size: 1 << 30,
            // chunk_size: 1 << 20,
            k: 1,
            num_block_packet: 0,
            degree: 0,
            group_size: 0,
        }
    }

    // let deploy = false;
    let deploy = true;
    let command = args().nth(1);

    if !deploy {
        // session(entropy(3333), &command, deploy).await?;
        // session(glacier(10000, 33), &command, deploy).await?;
        session(replication(33), &command, deploy).await?;
        return Ok(());
    }

    if command.as_deref() == Some("latency") {
        session(entropy(3333), &command, deploy).await?;
        session(glacier(10000, 33), &command, deploy).await?;
        session(replication(33), &command, deploy).await?
    }
    if command.as_deref() == Some("tput") {
        session(entropy(3333), &command, deploy).await?;
        session(glacier(10000, 33), &command, deploy).await?
    }
    if command.as_deref() == Some("scale-latency") {
        let mut spec = entropy(3333);
        for (f, num_block_packet) in [
            (333, 100),
            (667, 50),
            (1000, 33),
            (1333, 25),
            (1667, 20),
            // (2000, 16),
            (2100, 16),
            // (2333, 14),
            (2400, 14),
            // (2667, 12),
            (2800, 12),
            (3000, 11),
        ] {
            spec.f = f;
            spec.n = 3 * f + 1;
            spec.num_block_packet = num_block_packet;
            session(spec.clone(), &command, deploy).await?;
        }
        let mut spec = glacier(10000, 33);
        // for n in [1000, 2000, 3000, 4000, 5000, 6000, 7000, 8000, 9000] {
        for n in [1000, 2000, 4000, 6000, 8000] {
            spec.n = n;
            session(spec.clone(), &command, deploy).await?
        }
    }

    Ok(())
}

async fn session(
    mut spec: SystemSpec,
    command: &Option<String>,
    deploy: bool,
) -> anyhow::Result<()> {
    if deploy {
        anyhow::ensure!(spec.num_correct_packet() >= spec.k);
    }
    if spec.block_size() != 1 << 30
        || (command.as_deref() != Some("scale-latency")
            && spec.n
                != if matches!(spec.protocol, Replication) {
                    100
                } else {
                    10000
                })
    {
        if deploy {
            anyhow::bail!("incorrect spec")
        } else {
            println!("WARN nonstandard spec");
        }
    }

    let output = terraform_output().await?;
    anyhow::ensure!(output.instances().count() * 100 >= spec.n);
    let nodes = output.nodes().take(spec.n).collect::<Vec<_>>();

    let mut lines = String::new();
    if matches!(command.as_deref(), Some("latency" | "scale-latency")) {
        let mode = args().nth(2);
        let mode = match mode.as_deref() {
            Some("local") => LatencyMode::Local,
            Some("remote") => LatencyMode::Remote,
            _ => LatencyMode::Internal,
        };
        lines = latency_loop_session(&spec, mode, &nodes, deploy).await?
    } else if command.as_deref() == Some("tput") {
        spec.node_bandwidth = (10 << 20) / 8;
        lines = tput_loop_session(&spec, nodes, deploy).await?
    } else {
        reload(&spec).await?
    }

    if deploy {
        if let Some(command) = command {
            write(
                format!("./data/{}-{}.csv", command, UNIX_EPOCH.elapsed()?.as_secs()),
                lines,
            )
            .await?
        }
    }
    Ok(())
}

static CLIENT: LazyLock<reqwest::Client> = LazyLock::new(reqwest::Client::new);

fn ok_session(nodes: &[Node]) -> impl Future<Output = anyhow::Result<()>> + Send + 'static {
    let mut sessions = JoinSet::new();
    for node in nodes {
        let url = node.url();
        sessions.spawn(async move {
            let mut strike = 0;
            loop {
                sleep(Duration::from_millis(1000)).await;
                if let Err(err) = async {
                    CLIENT
                        .get(format!("{url}/ok"))
                        .timeout(Duration::from_millis(3000)) // why so much latency...
                        .send()
                        .await?
                        .error_for_status()?;
                    anyhow::Ok(())
                }
                .await
                {
                    strike += 1;
                    if strike > 1 {
                        println!("[{strike}/3] not ok {err}")
                    }
                    if strike == 3 {
                        return Err(err);
                    }
                } else {
                    strike = 0
                }
            }
        });
    }
    async move {
        sessions.join_next().await.expect("nodes not empty")??;
        anyhow::Ok(())
    }
}

#[derive(Debug, Clone, Copy)]
enum LatencyMode {
    Internal,
    Local,
    Remote,
}

async fn latency_loop_session(
    spec: &SystemSpec,
    mode: LatencyMode,
    nodes: &[Node],
    deploy: bool,
) -> anyhow::Result<String> {
    reload(spec).await?;
    sleep(Duration::from_secs(1)).await;
    let mut ok_session = pin!(ok_session(nodes));

    let mut lines = String::new();
    for i in 0..if deploy {
        if matches!(spec.protocol, Protocol::Replication) {
            10
        } else {
            30
        }
    } else {
        3
    } {
        let session = async {
            use LatencyMode::*;
            match mode {
                Internal => latency_session(spec.protocol, nodes, deploy).await,
                Local => latency_redirect_session(spec, nodes, true).await,
                Remote => latency_redirect_session(spec, nodes, false).await,
            }
        };
        let result = 'session: {
            tokio::select! {
                result = &mut ok_session => result?,
                result = session => break 'session result?,
            }
            anyhow::bail!("unreachable")
        };
        if i == 0 {
            continue;
        }
        for line in result {
            writeln!(&mut lines, "{},{line}", spec.csv_row())?
        }
    }
    Ok(lines)
}

async fn tput_loop_session(
    spec: &SystemSpec,
    nodes: Vec<Node>,
    deploy: bool,
) -> anyhow::Result<String> {
    let mut lines = String::new();
    let nodes = Arc::<[_]>::from(nodes);
    for &(count, concurrency) in if !deploy {
        &[(100, 100)][..]
        // &[(10, 10)][..]
    } else {
        &[
            (10, 1),
            (20, 2),
            (50, 5),
            (100, 10),
            (100, 20),
            (100, 40),
            (100, 60),
            (100, 80),
            (120, 120),
            (140, 140),
            (160, 160),
            (180, 180),
            (200, 200),
        ][..]
    } {
        reload(spec).await?;
        sleep(Duration::from_secs(1)).await;
        let mut ok_session = pin!(ok_session(&nodes));

        let result = 'session: {
            tokio::select! {
                result = &mut ok_session => result?,
                result = tput_session(spec.protocol, nodes.clone(), count, concurrency) => break 'session result?,
            }
            anyhow::bail!("unreachable")
        };
        writeln!(
            &mut lines,
            "{},{count},{concurrency},{result}",
            spec.csv_row()
        )?
    }
    Ok(lines)
}

async fn latency_session(
    protocol: Protocol,
    nodes: &[Node],
    deploy: bool,
) -> anyhow::Result<Vec<String>> {
    let namespace = protocol.namespace();
    let put_node = nodes
        .choose(&mut thread_rng())
        .ok_or(anyhow::format_err!("empty nodes"))?;
    println!("put @ {} {:?}", put_node.url(), put_node.instance.region());
    let (block_id, checksum, verifying_key) = CLIENT
        .post(format!("{}/{namespace}/put", put_node.url()))
        .send()
        .await?
        .error_for_status()?
        .json::<(String, u64, [u8; PUBLIC_KEY_LENGTH])>()
        .await?;
    println!("put {block_id} checksum {checksum:08x}");
    let put_latency = loop {
        sleep(Duration::from_secs(1)).await;
        if let Some(latency) = CLIENT
            .get(format!("{}/{namespace}/put/{block_id}", put_node.url()))
            .send()
            .await?
            .error_for_status()?
            .json::<Option<Duration>>()
            .await?
        {
            println!("{latency:?}");
            break latency;
        }
    };
    sleep(Duration::from_secs(if deploy { 10 } else { 1 })).await;

    let get_node = nodes
        .choose(&mut thread_rng())
        .ok_or(anyhow::format_err!("empty nodes"))?;
    println!(
        "get @ {} {:?} {block_id}",
        get_node.url(),
        get_node.instance.region()
    );
    loop {
        let response = CLIENT
            .post(format!("{}/{namespace}/get", get_node.url()))
            .json(&(block_id.clone(), verifying_key))
            .send()
            .await?;
        if response.status() == StatusCode::NOT_FOUND {
            println!("wait for dissemination");
            sleep(Duration::from_secs(1)).await;
            continue;
        }
        response.error_for_status()?;
        break;
    }
    let get_latency = loop {
        sleep(Duration::from_secs(1)).await;
        if let Some((latency, other_checksum)) = CLIENT
            .get(format!("{}/{namespace}/get/{block_id}", get_node.url()))
            .send()
            .await?
            .error_for_status()?
            .json::<Option<(Duration, u64)>>()
            .await?
        {
            println!("{latency:?} checksum {other_checksum:08x}");
            anyhow::ensure!(other_checksum == checksum);
            break latency;
        }
    };
    println!("wait for verifying post activity system stability");
    sleep(Duration::from_secs(1)).await;
    Ok(vec![
        format!("put,{}", put_latency.as_micros()),
        format!("get,{}", get_latency.as_micros()),
    ])
}

async fn latency_redirect_session(
    spec: &SystemSpec,
    nodes: &[Node],
    local: bool,
) -> anyhow::Result<Vec<String>> {
    let client_node = nodes
        .choose(&mut thread_rng())
        .ok_or(anyhow::format_err!("empty nodes"))?;
    let put_node = nodes
        .iter()
        .filter(|node| (node.instance.region() == client_node.instance.region()) == local)
        .choose(&mut thread_rng())
        .ok_or(anyhow::format_err!("empty nodes"))?;
    let get_node = nodes
        .iter()
        .filter(|node| (node.instance.region() == client_node.instance.region()) == local)
        .choose(&mut thread_rng())
        .ok_or(anyhow::format_err!("empty nodes"))?;
    println!(
        "client {} {:?}",
        client_node.url(),
        client_node.instance.region()
    );
    println!("  put {} {:?}", put_node.url(), put_node.instance.region());
    println!("  get {} {:?}", get_node.url(), get_node.instance.region());
    let redirect_spec = RedirectSpec {
        put_url: format!("{}/{}", put_node.url(), spec.protocol.namespace()),
        get_url: format!("{}/{}", get_node.url(), spec.protocol.namespace()),
        block_size: spec.block_size(),
    };
    let (put_latency, get_latency) = CLIENT
        .post(format!("{}/redirect", client_node.url()))
        .json(&redirect_spec)
        .send()
        .await?
        .error_for_status()?
        .json::<(Duration, Duration)>()
        .await?;
    println!("put {put_latency:?} get {get_latency:?}");
    let suffix = if local { "/local" } else { "/remote" };
    Ok(vec![
        format!("put{suffix},{}", put_latency.as_micros()),
        format!("get{suffix},{}", get_latency.as_micros()),
    ])
}

async fn tput_session(
    protocol: Protocol,
    nodes: Arc<[Node]>,
    count: usize,
    concurrency: usize,
) -> anyhow::Result<String> {
    let seq = Arc::new(AtomicUsize::new(0));
    let mut sessions = JoinSet::new();
    let start = Instant::now();
    for index in 0..concurrency {
        let nodes = nodes.clone();
        let seq = seq.clone();
        sessions.spawn(async move {
            let mut i;
            while {
                i = seq.fetch_add(1, SeqCst);
                i < count
            } {
                let put_url = nodes[i % nodes.len()].url();
                println!("[{index:03}] put {put_url}");
                let (block_id, _checksum, _verifying_key) = CLIENT
                    .post(format!("{}/{}/put", put_url, protocol.namespace()))
                    .send()
                    .await?
                    .error_for_status()?
                    .json::<(String, u64, [u8; PUBLIC_KEY_LENGTH])>()
                    .await?;
                loop {
                    if let Some(latency) = CLIENT
                        .get(format!(
                            "{}/{}/put/{block_id}",
                            put_url,
                            protocol.namespace()
                        ))
                        .send()
                        .await?
                        .error_for_status()?
                        .json::<Option<Duration>>()
                        .await?
                    {
                        println!("[{index:03}] {latency:?}");
                        break;
                    }
                }
            }
            anyhow::Ok(())
        });
    }
    join_all(sessions).await?;
    let total = start.elapsed();
    println!("total {total:?}");
    Ok(format!("{}", total.as_micros()))
}
