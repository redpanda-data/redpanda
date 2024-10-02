load("@rules_go//go:def.bzl", "go_binary", "go_library")

go_library(
    name = "setup-panic_lib",
    srcs = ["transform.go"],
    importpath = "github.com/redpanda-data/redpanda/src/transform-sdk/go/transform/internal/testdata/setup-panic",
    visibility = ["//visibility:private"],
)

go_binary(
    name = "setup-panic",
    embed = [":setup-panic_lib"],
    goarch = "wasm",
    goos = "wasip1",
    visibility = ["//src/v/wasm/tests:__pkg__"],
)
