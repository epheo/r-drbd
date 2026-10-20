// protox compiles the proto in-process so builds never need a protoc binary.
fn main() -> Result<(), Box<dyn std::error::Error>> {
    let fds = protox::compile(["proto/csi.proto"], ["proto"])?;
    tonic_prost_build::configure()
        .build_client(false)
        .build_server(true)
        .compile_fds(fds)?;
    println!("cargo:rerun-if-changed=proto/csi.proto");
    Ok(())
}
