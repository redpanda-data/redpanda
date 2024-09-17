load("@rules_go//go:def.bzl", "go_library")

go_library(
    name = "project",
    srcs = ["project.go"],
    importpath = "github.com/redpanda-data/redpanda/src/go/rpk/pkg/cli/transform/project",
    visibility = ["//visibility:public"],
    deps = [
        "@com_github_spf13_afero//:afero",
        "@in_gopkg_yaml_v3//:yaml_v3",
    ],
)
