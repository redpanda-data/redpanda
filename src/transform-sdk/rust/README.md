# Redpanda Data Transforms Rust SDK

This crate contains the SDK for Redpanda's inline Data Transforms, based on WebAssembly.

Data transforms let you run common data streaming tasks, like filtering, scrubbing, and transcoding, within Redpanda. For example, you may have consumers that require you to redact credit card numbers or convert JSON to Avro.

Data transforms use a WebAssembly (Wasm) engine inside a Redpanda broker. A Wasm function acts on a single record in an input topic. You can develop and manage data transforms with [`rpk transform`][rpk-transform] commands.

See [`examples/`][examples] for sample usages.

[rpk-transform]: https://docs.redpanda.com/beta/develop/data-transforms/run-transforms/
[examples]: https://github.com/redpanda-data/redpanda/tree/dev/src/transform-sdk/rust/examples
