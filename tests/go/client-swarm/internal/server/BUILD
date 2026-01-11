load("@rules_go//go:def.bzl", "go_library")

go_library(
    name = "server",
    srcs = ["server.go"],
    importpath = "github.com/redpanda-data/redpanda/tests/go/client-swarm/internal/server",
    visibility = ["//tests/go/client-swarm:__subpackages__"],
    deps = ["//tests/go/client-swarm/internal/metrics"],
)
