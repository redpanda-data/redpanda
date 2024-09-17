load("@rules_go//go:def.bzl", "go_library")

go_library(
    name = "adminapi",
    srcs = ["admin.go"],
    importpath = "github.com/redpanda-data/redpanda/src/go/rpk/pkg/adminapi",
    visibility = ["//visibility:public"],
    deps = [
        "//src/go/rpk/pkg/config",
        "//src/go/rpk/pkg/oauth",
        "//src/go/rpk/pkg/oauth/providers/auth0",
        "@com_github_redpanda_data_common_go_rpadmin//:rpadmin",
        "@com_github_spf13_afero//:afero",
    ],
)
