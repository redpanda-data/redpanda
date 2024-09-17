load("@rules_go//go:def.bzl", "go_library")

go_library(
    name = "acl",
    srcs = ["acl.go"],
    importpath = "github.com/redpanda-data/redpanda/src/go/rpk/pkg/cli/acl",
    visibility = ["//visibility:public"],
    deps = [
        "//src/go/rpk/pkg/cli/security/acl",
        "//src/go/rpk/pkg/cli/security/user",
        "//src/go/rpk/pkg/config",
        "@com_github_spf13_afero//:afero",
        "@com_github_spf13_cobra//:cobra",
        "@org_uber_go_zap//:zap",
    ],
)
