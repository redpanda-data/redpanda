load("@rules_go//go:def.bzl", "go_binary", "go_library")

go_library(
    name = "tee_lib",
    srcs = ["transform.go"],
    importpath = "github.com/redpanda-data/redpanda/src/transform-sdk/go/transform/internal/testdata/tee",
    visibility = ["//visibility:private"],
    deps = ["//src/transform-sdk/go/transform"],
)

go_binary(
    name = "tee",
    embed = [":tee_lib"],
    goarch = "wasm",
    goos = "wasip1",
    visibility = ["//src/v/wasm/tests:__pkg__"],
)
