use std::env::args;

use control::{join_all, ssh, terraform_output};
use tokio::task::JoinSet;

#[tokio::main(flavor = "current_thread")]
async fn main() -> anyhow::Result<()> {
    let rate = 1.;

    let output = terraform_output().await?;
    let mut sessions = JoinSet::new();
    for instance in output.instances() {
        let arg = args().nth(1);
        sessions.spawn(ssh(
            instance.public_dns.clone(),
            if arg.as_deref() != Some("unset") {
                format!(
                    "sudo tc qdisc add dev ens5 root tbf rate {rate}Gbit latency 50ms burst 10mb"
                ) + &format!(
                    "&& sudo tc qdisc add dev lo root tbf rate {}Gbit latency 50ms burst 10mb",
                    rate / 100.
                )
            } else {
                concat!(
                    "sudo tc qdisc del dev ens5 root",
                    "&& sudo tc qdisc del dev lo root",
                )
                .into()
            },
        ));
    }
    join_all(sessions).await
}
