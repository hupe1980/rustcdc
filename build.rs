//! Build script: compile `proto/event.proto` into a `FileDescriptorSet`.
//!
//! The descriptor is what makes Confluent Protobuf framing correct. That wire format
//! carries a **message-index path** — the position of the message within its `.proto`
//! file — and an index that happens to be wrong produces a header a Confluent
//! deserialiser misreads *without erroring*. `schemreg` therefore requires a real
//! `MessageDescriptor` rather than accepting hand-written indexes, and this is where that
//! descriptor comes from.
//!
//! Compiled with [`protox`], a **pure-Rust** protobuf compiler, so building rustcdc never
//! requires `protoc` on the machine. A library that needs a C++ toolchain to build is a
//! library people work around.
//!
//! Only runs under the `schemreg` feature; without it there is nothing to frame, and the
//! build-dependency is not pulled in at all.

fn main() {
    println!("cargo:rerun-if-changed=proto/event.proto");
    println!("cargo:rerun-if-changed=proto/event_key.proto");
    println!("cargo:rerun-if-changed=build.rs");

    #[cfg(feature = "schemreg")]
    {
        // Compiled separately, not as one descriptor set. The message-index path is
        // relative to the *file* a message lives in, and the schema registered for a
        // subject is that one file's source — so each descriptor pool must contain the
        // file it describes and nothing else.
        compile_descriptor("proto/event.proto", "event_descriptor.bin");
        compile_descriptor("proto/event_key.proto", "event_key_descriptor.bin");
    }
}

#[cfg(feature = "schemreg")]
fn compile_descriptor(proto: &str, output: &str) {
    let out_dir = std::path::PathBuf::from(
        std::env::var_os("OUT_DIR").expect("cargo always sets OUT_DIR for a build script"),
    );

    let descriptor_set = protox::compile([proto], ["."])
        .unwrap_or_else(|error| panic!("failed to compile {proto}: {error}"));

    let encoded = prost::Message::encode_to_vec(&descriptor_set);
    std::fs::write(out_dir.join(output), encoded)
        .expect("failed to write the compiled descriptor set");
}
