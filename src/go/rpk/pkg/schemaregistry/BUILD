load("@rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "schemaregistry",
    srcs = [
        "client.go",
        "context.go",
    ],
    importpath = "github.com/redpanda-data/redpanda/src/go/rpk/pkg/schemaregistry",
    visibility = ["//visibility:public"],
    deps = [
        "//src/go/rpk/pkg/adminapi",
        "//src/go/rpk/pkg/config",
        "//src/go/rpk/pkg/netutil",
        "//src/go/rpk/pkg/oauth/authtoken",
        "//src/go/rpk/pkg/oauth/providers/auth0",
        "@com_github_redpanda_data_common_go_rpsr//:rpsr",
        "@com_github_spf13_afero//:afero",
        "@com_github_twmb_franz_go//pkg/kgo",
        "@com_github_twmb_franz_go_pkg_sr//:sr",
        "@com_github_twmb_franz_go_plugin_kzap//:kzap",
    ],
)

go_test(
    name = "schemaregistry_test",
    srcs = ["context_test.go"],
    embed = [":schemaregistry"],
    deps = ["@com_github_stretchr_testify//require"],
)
