load("@rules_go//go:def.bzl", "go_library")

go_library(
    name = "version",
    srcs = ["version.go"],
    importpath = "github.com/redpanda-data/redpanda/src/go/rpk/pkg/cli/version",
    visibility = ["//visibility:public"],
    deps = [
        "//src/go/rpk/pkg/adminapi",
        "//src/go/rpk/pkg/config",
        "@com_github_redpanda_data_common_go_rpadmin//:rpadmin",
        "@com_github_spf13_afero//:afero",
        "@com_github_spf13_cobra//:cobra",
        "@org_uber_go_zap//:zap",
    ],
)
