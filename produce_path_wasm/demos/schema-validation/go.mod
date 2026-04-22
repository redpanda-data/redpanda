module schema-validator

go 1.21

require (
	github.com/actgardner/gogen-avro/v10 v10.2.1
	github.com/redpanda-data/redpanda/src/transform-sdk/go/transform v0.0.0
)

replace github.com/redpanda-data/redpanda/src/transform-sdk/go/transform => ../../../src/transform-sdk/go/transform
