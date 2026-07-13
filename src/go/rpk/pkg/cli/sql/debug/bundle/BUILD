load("@rules_go//go:def.bzl", "go_library")

go_library(
    name = "bundle",
    srcs = [
        "bundle.go",
        "client.go",
        "collect.go",
        "messages.go",
    ],
    importpath = "github.com/redpanda-data/redpanda/src/go/rpk/pkg/cli/sql/debug/bundle",
    visibility = ["//visibility:public"],
    deps = [
        "//src/go/rpk/pkg/cli/debug/debugbundle",
        "//src/go/rpk/pkg/config",
        "//src/go/rpk/pkg/httpapi",
        "//src/go/rpk/pkg/out",
        "@com_github_spf13_afero//:afero",
        "@com_github_spf13_cobra//:cobra",
        "@com_github_spf13_pflag//:pflag",
    ],
)
