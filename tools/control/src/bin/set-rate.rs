use std::env::args;

use control::{join_all, ssh, terraform_output};
use tokio::task::JoinSet;

#[tokio::main(flavor = "current_thread")]
async fn main() -> anyhow::Result<()> {
    let output = terraform_output().await?;
    let mut sessions = JoinSet::new();
    for instance in output.instances() {
        let arg = args().nth(1);
        sessions.spawn(ssh(
            instance.public_dns.clone(),
            if arg.as_deref() != Some("unset") {
                "sudo tc qdisc add dev ens5 root tbf rate 2.5Gbit latency 50ms burst 10mb"
            } else {
                "sudo tc qdisc del dev ens5 root"
            },
        ));
    }
    join_all(sessions).await
}
