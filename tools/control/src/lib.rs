use std::{
    collections::{BTreeMap, HashMap},
    net::IpAddr,
    path::Path,
    time::Duration,
};

use control_spec::SystemSpec;
use serde::Deserialize;
use tokio::{fs::write, process::Command, task::JoinSet, time::sleep};

#[derive(Debug, Clone, Deserialize)]
pub struct Instance {
    pub public_ip: IpAddr,
    pub private_ip: IpAddr,
    pub public_dns: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct TerraformOutput {
    pub regions: BTreeMap<String, Vec<Instance>>,
}

impl TerraformOutput {
    pub fn instances(&self) -> impl Iterator<Item = &Instance> + Clone {
        self.regions.values().flatten()
    }
}

pub struct Node {
    pub instance: Instance,
    pub local_index: usize,
}

impl Node {
    pub fn url(&self) -> String {
        format!(
            "http://{}:{}",
            self.instance.public_dns,
            self.local_index + 3000
        )
    }
}

impl TerraformOutput {
    pub fn nodes(&self) -> impl Iterator<Item = Node> + Clone + '_ {
        (0..).flat_map(|local_index| {
            self.instances().map(move |instance| Node {
                instance: instance.clone(),
                local_index,
            })
        })
    }
}

pub async fn terraform_output() -> anyhow::Result<TerraformOutput> {
    let output = Command::new("terraform")
        .args(["-chdir=tools/terraform", "output", "-json", "instances"])
        .output()
        .await?
        .stdout;
    Ok(serde_json::from_slice(&output)?)
}

impl Instance {
    pub fn region(&self) -> Option<String> {
        self.public_dns.split('.').nth(1).map(ToString::to_string)
    }
}

pub async fn ssh(host: impl AsRef<str>, command: impl AsRef<str>) -> anyhow::Result<()> {
    let status = Command::new("ssh")
        .arg(host.as_ref())
        .arg(command.as_ref())
        .stdout(std::process::Stdio::null())
        .status()
        .await?;
    anyhow::ensure!(status.success(), "{}", command.as_ref());
    Ok(())
}

pub async fn rsync(host: impl AsRef<str>, path: impl AsRef<Path>) -> anyhow::Result<()> {
    let status = Command::new("rsync")
        .arg(path.as_ref().display().to_string())
        .arg(format!("{}:", host.as_ref()))
        .status()
        .await?;
    anyhow::ensure!(status.success());
    Ok(())
}

pub async fn reload(spec: &SystemSpec) -> anyhow::Result<()> {
    write("./target/spec.json", &serde_json::to_vec_pretty(spec)?).await?;

    let output = terraform_output().await?;
    let mut sessions = JoinSet::new();
    for instance in output.instances() {
        sessions.spawn(ssh(
            instance.public_dns.clone(),
            "rm -r /tmp/entropy; pkill -x entropy-app; true",
        ));
        sessions.spawn(rsync(
            instance.public_dns.clone(),
            Path::new("target/spec.json"),
        ));
    }
    let mut the_result = Ok(());
    while let Some(result) = sessions.join_next().await {
        the_result = the_result.and_then(|_| anyhow::Ok(result??))
    }
    the_result?;
    println!("cleanup done");
    sleep(Duration::from_secs(1)).await;

    // for (index, node) in output.nodes().take(spec.n).enumerate() {
    //     let command = format!(
    //         "tmux new -d -s entropy-{index} \"./entropy-app {index} 2>./entropy-{index}.log\""
    //     );
    //     sessions.spawn(ssh(node.instance.public_dns.clone(), command));
    // }
    let mut instance_commands = HashMap::<_, Vec<_>>::new();
    for (index, node) in output.nodes().take(spec.n).enumerate() {
        let command = format!(
            "tmux new -d -s entropy-{index} \"./entropy-app {index} 2>./entropy-{index}.log\""
        );
        instance_commands
            .entry(node.instance.public_dns.clone())
            .or_default()
            .push(command);
    }
    for (host, commands) in instance_commands {
        sessions.spawn(async move {
            // for command in commands {
            //     ssh(&host, command).await?
            // }
            // Ok(())
            ssh(&host, commands.join(" && ")).await
        });
    }
    let mut the_result = Ok(());
    while let Some(result) = sessions.join_next().await {
        the_result = the_result.and_then(|_| anyhow::Ok(result??))
    }
    the_result
}
