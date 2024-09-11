use std::{
    env::args,
    future::Future,
    pin::pin,
    sync::{
        atomic::{AtomicUsize, Ordering::SeqCst},
        Arc, LazyLock,
    },
    time::Duration,
};

use control::{join_all, reload, terraform_output, Node};
use control_spec::SystemSpec;
use rand::{seq::SliceRandom, thread_rng};
use tokio::{
    task::JoinSet,
    time::{sleep, Instant},
};

const PUBLIC_KEY_LENGTH: usize = 32;

#[tokio::main(flavor = "current_thread")]
async fn main() -> anyhow::Result<()> {
    let spec = SystemSpec {
        n: 10000,
        f: 3333,
        num_block_packet: 10,
        block_size: 1 << 30,
        chunk_size: 32 << 10,
        degree: 6,
        // degree: 8,
    };
    let test = true;

    let is_multiple = spec.block_size % spec.chunk_size == 0;
    anyhow::ensure!(is_multiple);
    let k = spec.block_size / spec.chunk_size;
    let correct_packet_count = (spec.n - spec.f * 2) * spec.num_block_packet * spec.chunk_size;
    anyhow::ensure!(correct_packet_count >= k);
    if spec.block_size != 1 << 30 || spec.n != 10000 {
        if !test {
            anyhow::bail!("incorrect spec")
        } else {
            println!("WARN nonstandard spec")
        }
    }

    let output = terraform_output().await?;
    anyhow::ensure!(output.instances().count() * 100 >= spec.n);
    let nodes = output.nodes().take(spec.n).collect::<Vec<_>>();

    let command = args().nth(1);
    if command.as_deref() == Some("latency") {
        reload(&spec).await?;
        sleep(Duration::from_secs(1)).await;
        let mut ok_session = pin!(ok_session(&nodes));

        for _ in 0..10 {
            tokio::select! {
                result = &mut ok_session => result?,
                result = latency_session(&nodes) => result.map(|_| ())?,
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

async fn latency_session(nodes: &[Node]) -> anyhow::Result<Vec<String>> {
    let put_node = nodes
        .choose(&mut thread_rng())
        .ok_or(anyhow::format_err!("empty nodes"))?;
    println!("put @ {}", put_node.url());
    let (block_id, checksum, verifying_key) = CLIENT
        .post(format!("{}/entropy/put", put_node.url()))
        .send()
        .await?
        .error_for_status()?
        .json::<(String, u64, [u8; PUBLIC_KEY_LENGTH])>()
        .await?;
    println!("put {block_id} checksum {checksum:08x}");
    let put_latency = loop {
        if let Some(latency) = CLIENT
            .get(format!("{}/entropy/put/{block_id}", put_node.url()))
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
    CLIENT
        .post(format!("{}/entropy/get", get_node.url()))
        .json(&(block_id.clone(), verifying_key))
        .send()
        .await?
        .error_for_status()?;
    let get_latency = loop {
        if let Some((latency, other_checksum)) = CLIENT
            .get(format!("{}/entropy/get/{block_id}", get_node.url()))
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
