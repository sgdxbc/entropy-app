use std::{env::args, sync::LazyLock, time::Duration};

use control::{reload, terraform_output, Node};
use control_spec::SystemSpec;
use rand::{seq::SliceRandom, thread_rng};
use tokio::time::sleep;

const PUBLIC_KEY_LENGTH: usize = 32;

#[tokio::main(flavor = "current_thread")]
async fn main() -> anyhow::Result<()> {
    let spec = SystemSpec {
        n: 10000,
        f: 3333,
        num_block_packet: 10,
        chunk_size: 1 << 15,
        // degree: 6,
        degree: 8,
    };
    let block_size = (spec.n - spec.f * 2) * spec.num_block_packet * spec.chunk_size;
    if block_size != 1 << 30 || spec.n != 10000 {
        if args().count() > 1 {
            anyhow::bail!("incorrect spec")
        } else {
            println!("WARN nonstandard spec")
        }
    }

    reload(&spec).await?;
    sleep(Duration::from_secs(1)).await;

    let output = terraform_output().await?;
    let nodes = output.nodes().take(spec.n).collect::<Vec<_>>();
    for _ in 0..10 {
        latency_session(&nodes).await?
    }
    Ok(())
}

static CLIENT: LazyLock<reqwest::Client> = LazyLock::new(reqwest::Client::new);

async fn latency_session(nodes: &[Node]) -> anyhow::Result<()> {
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
    loop {
        if let Some(latency) = CLIENT
            .get(format!("{}/entropy/put/{block_id}", put_node.url()))
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
    sleep(Duration::from_secs(3)).await;

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
    loop {
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
            break;
        }
    }
    println!("wait for verifying post activity system stability");
    sleep(Duration::from_secs(3)).await;
    Ok(())
}
