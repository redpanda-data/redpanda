load("@rules_go//go:def.bzl", "go_library")

go_library(
    name = "connect",
    srcs = [
        "client.go",
        "connect.go",
        "install.go",
        "uninstall.go",
        "upgrade.go",
    ],
    importpath = "github.com/redpanda-data/redpanda/src/go/rpk/pkg/cli/connect",
    visibility = ["//visibility:public"],
    deps = [
        "//src/go/rpk/pkg/cobraext",
        "//src/go/rpk/pkg/config",
        "//src/go/rpk/pkg/httpapi",
        "//src/go/rpk/pkg/os",
        "//src/go/rpk/pkg/out",
        "//src/go/rpk/pkg/plugin",
        "//src/go/rpk/pkg/redpanda",
        "@com_github_spf13_afero//:afero",
        "@com_github_spf13_cobra//:cobra",
        "@org_uber_go_zap//:zap",
    ],
)
