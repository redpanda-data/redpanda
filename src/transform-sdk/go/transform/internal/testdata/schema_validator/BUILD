load("@rules_go//go:def.bzl", "go_binary", "go_library")

go_library(
    name = "schema_validator_lib",
    srcs = ["transform.go"],
    importpath = "github.com/redpanda-data/redpanda/src/transform-sdk/go/transform/internal/testdata/schema_validator",
    visibility = ["//visibility:private"],
    deps = [
        "//src/transform-sdk/go/transform",
        "//src/transform-sdk/go/transform/internal/testdata/schema-registry/avro",
        "//src/transform-sdk/go/transform/sr",
    ],
)

go_binary(
    name = "schema_validator",
    out = "schema_validator.wasm",
    embed = [":schema_validator_lib"],
    goarch = "wasm",
    goos = "wasip1",
    visibility = ["//src/v/wasm/tests:__pkg__"],
)
