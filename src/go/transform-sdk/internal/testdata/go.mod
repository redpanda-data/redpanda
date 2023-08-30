module github.com/redpanda-data/wasm-transform-testdata

go 1.20

require (
	github.com/actgardner/gogen-avro/v10 v10.2.1
	github.com/redpanda-data/redpanda/src/go/transform-sdk v0.0.0
)

replace github.com/redpanda-data/redpanda/src/go/transform-sdk => ../../
