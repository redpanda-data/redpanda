# vectorized wasm-api

Redpanda coprocessing client library for nodejs

## How to use?

1. Simply include the library and create your own instance of the SimpleTransform class.

```
const wasmJs = const {
  SimpleTransform,
} = require("@vectorizedio/wasm-api");
const transform = new SimpleTransform();
```

2. Specify what topics you want your script to consume from:

```
transform.subscribe(["my-topic", PolicyInjection.Stored]);
```

3. And write your custom transform logic that may filter or transform records in any way you'd like.

```
transform.processRecord((recordBatch) => {
   return Promise.resolve(new Map(....));
});
```

## Whats the expected output?

Use the map returned from the processRecord method to determine what output topics you'd like to produce onto. For example if the recordBatch arrived from 'my-topic' and your Map contains a single key 'Foo', the transformed recordBatch will be produced onto a materialized topic named 'my-topic.$Foo$'. Materialized topics share all topic attributes with its source topic.
