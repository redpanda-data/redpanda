load("@rules_go//go:def.bzl", "go_binary", "go_library")

go_library(
    name = "schema-registry_lib",
    srcs = ["transform.go"],
    importpath = "github.com/redpanda-data/redpanda/src/transform-sdk/go/transform/internal/testdata/schema-registry",
    visibility = ["//visibility:private"],
    deps = [
        "//src/transform-sdk/go/transform",
        "//src/transform-sdk/go/transform/internal/testdata/schema-registry/avro",
        "//src/transform-sdk/go/transform/sr",
    ],
)

go_binary(
    name = "schema-registry",
    embed = [":schema-registry_lib"],
    goarch = "wasm",
    goos = "wasip1",
    visibility = ["//src/v/wasm/tests:__pkg__"],
)
