load("@rules_go//go:def.bzl", "go_library")

go_library(
    name = "byoc",
    srcs = [
        "byoc.go",
        "install.go",
        "uninstall.go",
    ],
    importpath = "github.com/redpanda-data/redpanda/src/go/rpk/pkg/cli/cloud/byoc",
    visibility = ["//visibility:public"],
    deps = [
        "//src/go/rpk/pkg/cloudapi",
        "//src/go/rpk/pkg/cobraext",
        "//src/go/rpk/pkg/config",
        "//src/go/rpk/pkg/oauth",
        "//src/go/rpk/pkg/oauth/providers/auth0",
        "//src/go/rpk/pkg/os",
        "//src/go/rpk/pkg/out",
        "//src/go/rpk/pkg/plugin",
        "@com_github_spf13_afero//:afero",
        "@com_github_spf13_cobra//:cobra",
        "@org_uber_go_zap//:zap",
    ],
)
