load("@rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "verifier",
    srcs = [
        "client_helpers.go",
        "group_read_worker.go",
        "latest_value_map.go",
        "offset_ranges.go",
        "producer_worker.go",
        "random_read_worker.go",
        "seq_read_worker.go",
        "validator_status.go",
    ],
    importpath = "github.com/redpanda-data/redpanda/tests/go/kgo-verifier/pkg/worker/verifier",
    visibility = ["//visibility:public"],
    deps = [
        "//tests/go/kgo-verifier/pkg/util",
        "//tests/go/kgo-verifier/pkg/worker",
        "@com_github_google_uuid//:uuid",
        "@com_github_rcrowley_go_metrics//:go-metrics",
        "@com_github_sirupsen_logrus//:logrus",
        "@com_github_twmb_franz_go//pkg/kerr",
        "@com_github_twmb_franz_go//pkg/kgo",
        "@com_github_twmb_franz_go_pkg_kmsg//:kmsg",
        "@com_github_vectorizedio_redpanda_src_go_rpk//pkg/kafka",
        "@org_golang_x_sync//semaphore",
        "@org_golang_x_time//rate",
    ],
)

go_test(
    name = "verifier_test",
    srcs = [
        "offset_ranges_test.go",
        "validator_status_test.go",
    ],
    deps = [
        ":verifier",
        "@com_github_sirupsen_logrus//:logrus",
        "@com_github_stretchr_testify//assert",
        "@com_github_twmb_franz_go//pkg/kgo",
    ],
)
