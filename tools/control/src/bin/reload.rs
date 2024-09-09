use std::{env::args, path::Path};

use control::{rsync, ssh, terraform_output};
use tokio::{process::Command, task::JoinSet};

#[tokio::main(flavor = "current_thread")]
async fn main() -> anyhow::Result<()> {
    let args1 = args().nth(1);
    let sync = args1.as_deref() == Some("sync");
    if sync {
        println!("Building artifact");
        let status = Command::new("cargo")
            .args(["build", "--release", "--bin", "entropy-app"])
            .status()
            .await?;
        anyhow::ensure!(status.success(), "Command `cargo build` exit with {status}");
    }

    let output = terraform_output().await?;
    let mut sessions = JoinSet::new();
    for (index, instance) in output.regions.values().flatten().enumerate() {
        sessions.spawn(instance_session(instance.public_dns.clone(), index, sync));
    }
    let mut the_result = Ok(());
    while let Some(result) = sessions.join_next().await {
        the_result = the_result.and_then(|_| anyhow::Ok(result??))
    }
    the_result
}

async fn instance_session(ssh_host: String, index: usize, sync: bool) -> anyhow::Result<()> {
    if sync {
        rsync(&ssh_host, Path::new("target/release/entropy-app")).await?
    }
    rsync(&ssh_host, Path::new("target/spec.json")).await?;
    let command =
        format!("pkill -x entropy-app; sleep 1; tmux new -d -s entropy \"./entropy-app {index}\"");
    ssh(ssh_host, command).await
}
