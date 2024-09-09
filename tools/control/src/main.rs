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
    reload(spec).await?;
    sleep(Duration::from_secs(1)).await;

    let client = reqwest::Client::new();
    let output = terraform_output().await?;
    let instances = output.regions.into_values().flatten().collect::<Vec<_>>();
    let instance0 = instances.first().unwrap();
    for get_instance in &instances {
        println!("start");
        let (block_id, checksum, verifying_key) = client
            .post(format!("{}/entropy/put", instance0.url()))
            .send()
            .await?
            .error_for_status()?
            .json::<(String, u64, [u8; PUBLIC_KEY_LENGTH])>()
            .await?;
        println!("put {block_id} checksum {checksum:08x}");
        loop {
            if let Some(latency) = client
                .get(format!("{}/entropy/put/{block_id}", instance0.url()))
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
            .post(format!("{}/entropy/get", get_instance.url()))
            .json(&(block_id.clone(), verifying_key))
            .send()
            .await?
            .error_for_status()?;
        loop {
            if let Some((latency, other_checksum)) = client
                .get(format!("{}/entropy/get/{block_id}", get_instance.url()))
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
