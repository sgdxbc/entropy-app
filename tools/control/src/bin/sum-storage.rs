use control::terraform_output;
use tokio::{process::Command, task::JoinSet};

#[tokio::main(flavor = "current_thread")]
async fn main() -> anyhow::Result<()> {
    let output = terraform_output().await?;
    let mut sessions = JoinSet::new();
    for instance in output.instances() {
        sessions.spawn(
            Command::new("ssh")
                .args([&instance.public_dns, "du", "-s", "/tmp/entropy"])
                .output(),
        );
    }
    let mut size = 0;
    while let Some(result) = sessions.join_next().await {
        size += String::from_utf8(result??.stdout)?
            .split_ascii_whitespace()
            .next()
            .ok_or(anyhow::format_err!("expect size"))?
            .parse::<usize>()?
    }
    println!("{size}");
    Ok(())
}
