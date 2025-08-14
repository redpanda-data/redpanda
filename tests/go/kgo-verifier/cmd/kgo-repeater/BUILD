load("@rules_go//go:def.bzl", "go_binary", "go_library")

go_library(
    name = "kgo-repeater_lib",
    srcs = ["main.go"],
    importpath = "github.com/redpanda-data/redpanda/tests/go/kgo-verifier/cmd/kgo-repeater",
    visibility = ["//visibility:private"],
    deps = [
        "//tests/go/kgo-verifier/pkg/util",
        "//tests/go/kgo-verifier/pkg/worker",
        "//tests/go/kgo-verifier/pkg/worker/repeater",
        "@com_github_sirupsen_logrus//:logrus",
        "@com_github_twmb_franz_go//pkg/kgo",
        "@com_github_twmb_franz_go//pkg/sasl/scram",
        "@com_github_twmb_franz_go_pkg_kadm//:kadm",
    ],
)

go_binary(
    name = "kgo-repeater",
    embed = [":kgo-repeater_lib"],
    visibility = ["//visibility:public"],
)
