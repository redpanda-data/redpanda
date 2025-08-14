load("@rules_go//go:def.bzl", "go_binary", "go_library")

go_library(
    name = "kgo-verifier_lib",
    srcs = ["main.go"],
    importpath = "github.com/redpanda-data/redpanda/tests/go/kgo-verifier/cmd/kgo-verifier",
    visibility = ["//visibility:private"],
    deps = [
        "//tests/go/kgo-verifier/pkg/util",
        "//tests/go/kgo-verifier/pkg/worker",
        "//tests/go/kgo-verifier/pkg/worker/verifier",
        "@com_github_sirupsen_logrus//:logrus",
        "@com_github_twmb_franz_go//pkg/kerr",
        "@com_github_twmb_franz_go//pkg/kgo",
        "@com_github_twmb_franz_go_pkg_kmsg//:kmsg",
    ],
)

go_binary(
    name = "kgo-verifier",
    embed = [":kgo-verifier_lib"],
    visibility = ["//visibility:public"],
)
