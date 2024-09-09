use control::reload;
use control_spec::SystemSpec;
use tokio::fs::write;

#[tokio::main(flavor = "current_thread")]
async fn main() -> anyhow::Result<()> {
    let spec = SystemSpec {
        n: 31,
        f: 10,
        num_block_packet: 10,
        chunk_size: 1 << 10,
    };
    write("./target/spec.json", &serde_json::to_vec_pretty(&spec)?).await?;
    reload().await
}
