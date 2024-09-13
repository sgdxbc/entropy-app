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
    SystemSpec,
};
use rand::{seq::SliceRandom, thread_rng};
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
            protocol: Entropy,
            chunk_size: 32 << 10,
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
            protocol: Replication,
            // chunk_size: 1 << 30,
            chunk_size: 64 << 20,
            k: 1,
            num_block_packet: 0,
            degree: 0,
            group_size: 0,
        }
    }

    // let spec = entropy(333);
    // let spec = glacier(1000, 33);
    let spec = replication(333);
    let deploy = false;

    if deploy {
        anyhow::ensure!(spec.num_correct_packet() >= spec.k);
    }
    if spec.block_size() != 1 << 30 || spec.n != 10000 {
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
    let command = args().nth(1);
    if command.as_deref() == Some("latency") {
        reload(&spec).await?;
        sleep(Duration::from_secs(1)).await;
        let mut ok_session = pin!(ok_session(&nodes));

        for _ in 0..if deploy { 10 } else { 3 } {
            let result = 'session: {
                tokio::select! {
                    result = &mut ok_session => result?,
                    result = latency_session(spec.protocol, &nodes) => break 'session result?,
                }
                anyhow::bail!("unreachable")
            };
            for line in result {
                writeln!(&mut lines, "{},{line}", spec.csv_row())?
            }
        }
    } else if command.as_deref() == Some("tput") {
        reload(&spec).await?;
        sleep(Duration::from_secs(1)).await;
        let mut ok_session = pin!(ok_session(&nodes));

        tokio::select! {
            result = &mut ok_session => result?,
            result = tput_session(nodes, 8, 4) => result.map(|_| ())?,
        }
    } else {
        reload(&spec).await?;
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
                        .timeout(Duration::from_millis(2000))
                        .send()
                        .await?
                        .error_for_status()?;
                    anyhow::Ok(())
                }
                .await
                {
                    strike += 1;
                    println!("[{strike}/3] not ok {err}");
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

async fn latency_session(protocol: Protocol, nodes: &[Node]) -> anyhow::Result<Vec<String>> {
    let namespace = protocol.namespace();
    let put_node = nodes
        .choose(&mut thread_rng())
        .ok_or(anyhow::format_err!("empty nodes"))?;
    println!("put @ {}", put_node.url());
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
    sleep(Duration::from_secs(1)).await;

    let get_node = nodes
        .choose(&mut thread_rng())
        .ok_or(anyhow::format_err!("empty nodes"))?;
    println!("get @ {} {block_id}", get_node.url());
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

async fn tput_session(
    nodes: Vec<Node>,
    count: usize,
    concurrency: usize,
) -> anyhow::Result<String> {
    let nodes = Arc::<[_]>::from(nodes);
    let count = Arc::new(AtomicUsize::new(count + concurrency));
    let mut sessions = JoinSet::new();
    let start = Instant::now();
    for index in 0..concurrency {
        let nodes = nodes.clone();
        let count = count.clone();
        sessions.spawn(async move {
            let mut i;
            while {
                i = count.fetch_sub(1, SeqCst);
                i > concurrency
            } {
                let put_url = nodes[i % nodes.len()].url();
                println!("[{index:02}] put {put_url}");
                let (block_id, _checksum, _verifying_key) = CLIENT
                    .post(format!("{}/entropy/put", put_url))
                    .send()
                    .await?
                    .error_for_status()?
                    .json::<(String, u64, [u8; PUBLIC_KEY_LENGTH])>()
                    .await?;
                loop {
                    if let Some(latency) = CLIENT
                        .get(format!("{}/entropy/put/{block_id}", put_url))
                        .send()
                        .await?
                        .error_for_status()?
                        .json::<Option<Duration>>()
                        .await?
                    {
                        println!("[{index:02}] {latency:?}");
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
