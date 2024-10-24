load("@rules_go//go:def.bzl", "go_library")

go_library(
    name = "cloud",
    srcs = [
        "cloud.go",
        "login.go",
        "logout.go",
    ],
    importpath = "github.com/redpanda-data/redpanda/src/go/rpk/pkg/cli/cloud",
    visibility = ["//visibility:public"],
    deps = [
        "//src/go/rpk/pkg/cli/cloud/auth",
        "//src/go/rpk/pkg/cli/cloud/byoc",
        "//src/go/rpk/pkg/cli/cloud/cluster",
        "//src/go/rpk/pkg/cli/cloud/resourcegroup",
        "//src/go/rpk/pkg/cli/container/common",
        "//src/go/rpk/pkg/cli/profile",
        "//src/go/rpk/pkg/config",
        "//src/go/rpk/pkg/oauth",
        "//src/go/rpk/pkg/oauth/providers/auth0",
        "//src/go/rpk/pkg/os",
        "//src/go/rpk/pkg/out",
        "@com_github_spf13_afero//:afero",
        "@com_github_spf13_cobra//:cobra",
    ],
)
