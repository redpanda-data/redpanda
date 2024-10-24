load("@rules_go//go:def.bzl", "go_library")

go_library(
    name = "testfs",
    srcs = ["testfs.go"],
    importpath = "github.com/redpanda-data/redpanda/src/go/rpk/pkg/testfs",
    visibility = ["//visibility:public"],
    deps = [
        "@com_github_spf13_afero//:afero",
        "@org_golang_x_exp//maps",
    ],
)
