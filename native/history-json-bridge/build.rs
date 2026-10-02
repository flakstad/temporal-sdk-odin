use prost::Message;
use std::{env, fs, path::PathBuf};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let manifest_dir = PathBuf::from(env::var("CARGO_MANIFEST_DIR")?);
    let api_root = manifest_dir.join("../sdk-core/crates/protos/protos/api_upstream");
    let history_proto = api_root.join("temporal/api/history/v1/message.proto");
    println!("cargo:rerun-if-changed={}", api_root.display());

    let descriptors = protox::compile([history_proto], [api_root])?;
    let output = PathBuf::from(env::var("OUT_DIR")?).join("history-descriptors.bin");
    fs::write(output, descriptors.encode_to_vec())?;
    Ok(())
}
