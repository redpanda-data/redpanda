load("@rules_go//go:def.bzl", "go_library")

go_library(
    name = "cpu",
    srcs = ["tuner.go"],
    importpath = "github.com/redpanda-data/redpanda/src/go/rpk/pkg/tuners/cpu",
    visibility = ["//visibility:public"],
    deps = [
        "//src/go/rpk/pkg/system",
        "//src/go/rpk/pkg/tuners",
        "//src/go/rpk/pkg/tuners/executors",
        "//src/go/rpk/pkg/tuners/executors/commands",
        "//src/go/rpk/pkg/tuners/irq",
        "//src/go/rpk/pkg/utils",
        "@com_github_spf13_afero//:afero",
        "@org_uber_go_zap//:zap",
    ],
)
