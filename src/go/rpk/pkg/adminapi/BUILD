load("@rules_go//go:def.bzl", "go_library", "go_test")

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
        "@org_uber_go_zap//:zap",
    ],
)

go_test(
    name = "adminapi_test",
    size = "small",
    srcs = ["admin_test.go"],
    embed = [":adminapi"],
    env = {"HOME": "/"},
    deps = [
        "//src/go/rpk/pkg/config",
        "@com_github_spf13_afero//:afero",
        "@com_github_stretchr_testify//require",
    ],
)
