load("@rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "versioncmd",
    srcs = ["versioncmd.go"],
    importpath = "github.com/redpanda-data/redpanda/src/go/rpk/pkg/cli/version/versioncmd",
    visibility = ["//visibility:public"],
    deps = [
        "//src/go/rpk/pkg/adminapi",
        "//src/go/rpk/pkg/cli/version",
        "//src/go/rpk/pkg/config",
        "@com_github_redpanda_data_common_go_rpadmin//:rpadmin",
        "@com_github_spf13_afero//:afero",
        "@com_github_spf13_cobra//:cobra",
        "@org_uber_go_zap//:zap",
    ],
)

go_test(
    name = "versioncmd_test",
    srcs = ["versioncmd_test.go"],
    deps = [
        ":versioncmd",
        "//src/go/rpk/pkg/config",
        "@com_github_spf13_afero//:afero",
        "@com_github_stretchr_testify//require",
    ],
)
