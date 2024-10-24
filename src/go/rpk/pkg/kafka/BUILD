load("@rules_go//go:def.bzl", "go_library")

go_library(
    name = "kafka",
    srcs = ["client_franz.go"],
    importpath = "github.com/redpanda-data/redpanda/src/go/rpk/pkg/kafka",
    visibility = ["//visibility:public"],
    deps = [
        "//src/go/rpk/pkg/adminapi",
        "//src/go/rpk/pkg/config",
        "//src/go/rpk/pkg/oauth",
        "//src/go/rpk/pkg/oauth/providers/auth0",
        "@com_github_spf13_afero//:afero",
        "@com_github_twmb_franz_go//pkg/kerr",
        "@com_github_twmb_franz_go//pkg/kgo",
        "@com_github_twmb_franz_go//pkg/sasl/oauth",
        "@com_github_twmb_franz_go//pkg/sasl/plain",
        "@com_github_twmb_franz_go//pkg/sasl/scram",
        "@com_github_twmb_franz_go_pkg_kadm//:kadm",
        "@com_github_twmb_franz_go_pkg_kmsg//:kmsg",
        "@com_github_twmb_franz_go_plugin_kzap//:kzap",
    ],
)
