module github.com/redpanda-data/redpanda/src/transform-sdk/go/transform/internal/testdata

go 1.20

require (
	github.com/actgardner/gogen-avro/v10 v10.2.1
	github.com/redpanda-data/redpanda/src/transform-sdk/go/transform v0.0.1
)

replace github.com/redpanda-data/redpanda/src/transform-sdk/go/transform => ../../
