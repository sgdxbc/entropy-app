use std::{env::args, net::SocketAddr, path::Path};

use control::{rsync, terraform_output};
use tokio::{fs::write, task::JoinSet};

#[tokio::main(flavor = "current_thread")]
async fn main() -> anyhow::Result<()> {
    #[derive(Debug, Clone, Copy)]
    enum Mode {
        Latency,
        Tput,
    }
    let mode = match args().nth(1).as_deref() {
        Some("latency") => Mode::Latency,
        Some("tput") => Mode::Tput,
        _ => anyhow::bail!("unimplemented"),
    };

    let output = terraform_output().await?;
    let addrs = (0..100)
        .flat_map(|i| {
            output
                .regions
                .values()
                .flatten()
                .map(|instance| match mode {
                    Mode::Latency => instance.public_ip,
                    Mode::Tput => instance.private_ip,
                })
                .map(move |ip| SocketAddr::from((ip, 3000 + i)))
        })
        .collect::<Vec<_>>();

    let addrs_path = Path::new("./target/addrs.json");
    write(addrs_path, serde_json::to_vec_pretty(&addrs)?).await?;

    let mut sessions = JoinSet::new();
    for instance in output.regions.values().flatten() {
        sessions.spawn(rsync(instance.public_dns.clone(), addrs_path));
    }
    let mut the_result = Ok(());
    while let Some(result) = sessions.join_next().await {
        the_result = the_result.and_then(|_| anyhow::Ok(result??))
    }
    the_result
}
