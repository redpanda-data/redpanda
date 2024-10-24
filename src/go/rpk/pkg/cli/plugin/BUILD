load("@rules_go//go:def.bzl", "go_library")

go_library(
    name = "plugin",
    srcs = [
        "install.go",
        "list.go",
        "plugin.go",
        "uninstall.go",
    ],
    importpath = "github.com/redpanda-data/redpanda/src/go/rpk/pkg/cli/plugin",
    visibility = ["//visibility:public"],
    deps = [
        "//src/go/rpk/pkg/os",
        "//src/go/rpk/pkg/out",
        "//src/go/rpk/pkg/plugin",
        "@com_github_spf13_afero//:afero",
        "@com_github_spf13_cobra//:cobra",
        "@org_uber_go_zap//:zap",
    ],
)
