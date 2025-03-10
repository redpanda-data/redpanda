load("@rules_go//go:def.bzl", "go_library")

go_library(
    name = "security",
    srcs = ["security.go"],
    importpath = "github.com/redpanda-data/redpanda/src/go/rpk/pkg/cli/cloud/security",
    visibility = ["//visibility:public"],
    deps = [
        "//src/go/rpk/pkg/cli/cloud/security/secret",
        "//src/go/rpk/pkg/config",
        "@com_github_spf13_afero//:afero",
        "@com_github_spf13_cobra//:cobra",
    ],
)
