load("@rules_go//go:def.bzl", "go_library")

go_library(
    name = "cmd",
    srcs = [
        "connections.go",
        "consumers.go",
        "producers.go",
        "root.go",
    ],
    importpath = "github.com/redpanda-data/redpanda/tests/go/client-swarm/cmd",
    visibility = ["//visibility:public"],
    deps = [
        "//tests/go/client-swarm/internal/connections",
        "//tests/go/client-swarm/internal/consumers",
        "//tests/go/client-swarm/internal/metrics",
        "//tests/go/client-swarm/internal/producers",
        "//tests/go/client-swarm/internal/server",
        "@com_github_spf13_cobra//:cobra",
    ],
)
