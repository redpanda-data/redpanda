load("@rules_go//go:def.bzl", "go_binary", "go_library")

go_library(
    name = "rpk_lib",
    srcs = ["main.go"],
    importpath = "github.com/redpanda-data/redpanda/src/go/rpk/cmd/rpk",
    visibility = ["//visibility:private"],
    deps = ["//src/go/rpk/pkg/cli"],
)

go_binary(
    name = "rpk",
    embed = [":rpk_lib"],
    visibility = ["//visibility:public"],
)
