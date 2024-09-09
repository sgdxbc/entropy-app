#[tokio::main(flavor = "current_thread")]
async fn main() -> anyhow::Result<()> {
    let output = control::terraform_output().await?;
    println!("{output:#?}");
    Ok(())
}
