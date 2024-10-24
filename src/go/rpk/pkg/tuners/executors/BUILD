load("@rules_go//go:def.bzl", "go_library")

go_library(
    name = "executors",
    srcs = [
        "direct.go",
        "executor.go",
        "script_rendering.go",
    ],
    importpath = "github.com/redpanda-data/redpanda/src/go/rpk/pkg/tuners/executors",
    visibility = ["//visibility:public"],
    deps = [
        "//src/go/rpk/pkg/tuners/executors/commands",
        "@com_github_spf13_afero//:afero",
    ],
)
