load("@rules_go//go:def.bzl", "go_library")

go_library(
    name = "producers",
    srcs = ["producers.go"],
    importpath = "github.com/redpanda-data/redpanda/tests/go/client-swarm/internal/producers",
    visibility = ["//tests/go/client-swarm:__subpackages__"],
    deps = [
        "//tests/go/client-swarm/internal/metrics",
        "@com_github_twmb_franz_go//pkg/kgo",
        "@org_golang_x_time//rate",
    ],
)
