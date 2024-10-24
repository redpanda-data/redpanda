load("@rules_go//go:def.bzl", "go_library")

go_library(
    name = "cluster",
    srcs = [
        "cluster.go",
        "select.go",
    ],
    importpath = "github.com/redpanda-data/redpanda/src/go/rpk/pkg/cli/cloud/cluster",
    visibility = ["//visibility:public"],
    deps = [
        "//src/go/rpk/pkg/cli/profile",
        "//src/go/rpk/pkg/config",
        "//src/go/rpk/pkg/out",
        "@com_github_spf13_afero//:afero",
        "@com_github_spf13_cobra//:cobra",
    ],
)
