fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("cargo:rerun-if-changed=../../../../../api/xenon/v1/persistence.proto");
    tonic_build::configure()
        .build_client(false)
        .boxed(".xenon.v1.StoredOutcome.result.execution_result")
        .compile_protos(
            &["../../../../../api/xenon/v1/persistence.proto"],
            &["../../../../../api"],
        )?;
    Ok(())
}
