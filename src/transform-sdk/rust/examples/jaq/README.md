# jaq transform

[`jaq`][jaq] is a Rust clone of the popular JSON processing tool `jq`.

This example transform supports defining an environment variable `FILTER` that defines a `jaq` filter, which is applied to all record values.

## Example Usage

```
cargo build --release --target=wasm32-wasi
rpk transform deploy ../../target/wasm32-wasi/release/jaq.wasm \
    --name=email-redaction-transform \
    --input-topic=$MY_INPUT_TOPIC \
    --output-topic=$MY_OUTPUT_TOPIC \
    --var 'FILTER=del(.email)'
echo '{"data":9,"email":"foo@bar.com"}' | rpk topic produce $MY_INPUT_TOPIC
rpk topic consume $MY_OUTPUT_TOPIC
# {"data":9}
```

[jaq]: https://github.com/01mf02/jaq
