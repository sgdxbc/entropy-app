use std::time::Duration;

use control::{reload, terraform_output};
use control_spec::SystemSpec;
use tokio::time::sleep;

const PUBLIC_KEY_LENGTH: usize = 32;

#[tokio::main(flavor = "current_thread")]
async fn main() -> anyhow::Result<()> {
    let spec = SystemSpec {
        n: 31,
        f: 10,
        num_block_packet: 10,
        chunk_size: 1 << 10,
    };
    reload(&spec).await?;
    sleep(Duration::from_secs(1)).await;

    let client = reqwest::Client::new();
    let output = terraform_output().await?;
    let nodes = output.nodes().take(spec.n).collect::<Vec<_>>();
    let node0 = nodes.first().ok_or(anyhow::format_err!("empty nodes"))?;
    for get_node in &nodes {
        println!("start");
        let (block_id, checksum, verifying_key) = client
            .post(format!("{}/entropy/put", node0.url()))
            .send()
            .await?
            .error_for_status()?
            .json::<(String, u64, [u8; PUBLIC_KEY_LENGTH])>()
            .await?;
        println!("put {block_id} checksum {checksum:08x}");
        loop {
            if let Some(latency) = client
                .get(format!("{}/entropy/put/{block_id}", node0.url()))
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
            .post(format!("{}/entropy/get", get_node.url()))
            .json(&(block_id.clone(), verifying_key))
            .send()
            .await?
            .error_for_status()?;
        loop {
            if let Some((latency, other_checksum)) = client
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
        sleep(Duration::from_secs(1)).await;
    }

    Ok(())
}
