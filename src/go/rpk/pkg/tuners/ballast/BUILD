load("@rules_go//go:def.bzl", "go_library")

go_library(
    name = "ballast",
    srcs = ["tuner.go"],
    importpath = "github.com/redpanda-data/redpanda/src/go/rpk/pkg/tuners/ballast",
    visibility = ["//visibility:public"],
    deps = select({
        "@rules_go//go/platform:android": [
            "//src/go/rpk/pkg/config",
            "//src/go/rpk/pkg/tuners",
            "//src/go/rpk/pkg/tuners/executors",
            "//src/go/rpk/pkg/tuners/executors/commands",
            "@com_github_docker_go_units//:go-units",
        ],
        "@rules_go//go/platform:linux": [
            "//src/go/rpk/pkg/config",
            "//src/go/rpk/pkg/tuners",
            "//src/go/rpk/pkg/tuners/executors",
            "//src/go/rpk/pkg/tuners/executors/commands",
            "@com_github_docker_go_units//:go-units",
        ],
        "//conditions:default": [],
    }),
)
