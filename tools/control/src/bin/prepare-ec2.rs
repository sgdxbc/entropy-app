use std::{env::args, net::SocketAddr, path::Path};

use control::{join_all, rsync, terraform_output};
use tokio::{fs::write, task::JoinSet};

#[tokio::main(flavor = "current_thread")]
async fn main() -> anyhow::Result<()> {
    #[derive(Debug, Clone, Copy)]
    enum Mode {
        Public,
        Private,
    }
    let mode = match args().nth(1).as_deref() {
        Some("public") => Mode::Public,
        Some("private") => Mode::Private,
        _ => anyhow::bail!("unimplemented"),
    };

    let output = terraform_output().await?;
    let addrs = output
        .nodes()
        .take_while(|node| node.local_index < 100)
        .map(|node| {
            let ip = match mode {
                Mode::Public => node.instance.public_ip,
                Mode::Private => node.instance.private_ip,
            };
            (
                SocketAddr::from((ip, (node.local_index + 3000) as _)),
                node.instance.region(),
                node.url(),
            )
        })
        .collect::<Vec<_>>();

    let addrs_path = Path::new("./target/addrs.json");
    write(addrs_path, serde_json::to_vec_pretty(&addrs)?).await?;

    let mut sessions = JoinSet::new();
    for instance in output.instances() {
        sessions.spawn(rsync(instance.public_dns.clone(), addrs_path));
    }
    join_all(sessions).await
}
