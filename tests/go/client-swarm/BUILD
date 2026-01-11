load("@rules_go//go:def.bzl", "go_binary", "go_library")

go_library(
    name = "client-swarm_lib",
    srcs = ["main.go"],
    importpath = "github.com/redpanda-data/redpanda/tests/go/client-swarm",
    visibility = ["//visibility:private"],
    deps = ["//tests/go/client-swarm/cmd"],
)

go_binary(
    name = "client-swarm",
    embed = [":client-swarm_lib"],
    visibility = ["//visibility:public"],
)
