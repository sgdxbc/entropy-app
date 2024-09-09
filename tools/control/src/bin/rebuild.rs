use std::path::Path;

use control::{rsync, terraform_output};
use tokio::{process::Command, task::JoinSet};

#[tokio::main(flavor = "current_thread")]
async fn main() -> anyhow::Result<()> {
    println!("Building artifact");
    let status = Command::new("cargo")
        .args(["build", "--release", "--bin", "entropy-app"])
        .status()
        .await?;
    anyhow::ensure!(status.success(), "Command `cargo build` exit with {status}");

    let output = terraform_output().await?;
    let mut sessions = JoinSet::new();
    for instance in output.instances() {
        sessions.spawn(rsync(
            instance.public_dns.clone(),
            Path::new("target/release/entropy-app"),
        ));
    }
    let mut the_result = Ok(());
    while let Some(result) = sessions.join_next().await {
        the_result = the_result.and_then(|_| anyhow::Ok(result??))
    }
    the_result
}
