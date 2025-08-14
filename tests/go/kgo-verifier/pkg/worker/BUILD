load("@rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "worker",
    srcs = [
        "transaction_stm.go",
        "worker.go",
    ],
    importpath = "github.com/redpanda-data/redpanda/tests/go/kgo-verifier/pkg/worker",
    visibility = ["//visibility:public"],
    deps = [
        "@com_github_rcrowley_go_metrics//:go-metrics",
        "@com_github_sirupsen_logrus//:logrus",
        "@com_github_twmb_franz_go//pkg/kgo",
        "@com_github_twmb_franz_go//pkg/sasl/scram",
    ],
)

go_test(
    name = "worker_test",
    srcs = ["worker_test.go"],
    embed = [":worker"],
)
