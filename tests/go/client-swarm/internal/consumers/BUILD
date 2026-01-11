load("@rules_go//go:def.bzl", "go_library")

go_library(
    name = "consumers",
    srcs = ["consumers.go"],
    importpath = "github.com/redpanda-data/redpanda/tests/go/client-swarm/internal/consumers",
    visibility = ["//tests/go/client-swarm:__subpackages__"],
    deps = [
        "//tests/go/client-swarm/internal/metrics",
        "@com_github_twmb_franz_go//pkg/kgo",
    ],
)
