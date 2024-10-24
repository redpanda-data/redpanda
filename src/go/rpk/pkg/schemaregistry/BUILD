load("@rules_go//go:def.bzl", "go_library")

go_library(
    name = "schemaregistry",
    srcs = ["client.go"],
    importpath = "github.com/redpanda-data/redpanda/src/go/rpk/pkg/schemaregistry",
    visibility = ["//visibility:public"],
    deps = [
        "//src/go/rpk/pkg/config",
        "//src/go/rpk/pkg/net",
        "@com_github_spf13_afero//:afero",
        "@com_github_twmb_franz_go_pkg_sr//:sr",
    ],
)
