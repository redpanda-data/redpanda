load("@rules_go//go:def.bzl", "go_library")

go_library(
    name = "repeater",
    srcs = ["repeater_worker.go"],
    importpath = "github.com/redpanda-data/redpanda/tests/go/kgo-verifier/pkg/worker/repeater",
    visibility = ["//visibility:public"],
    deps = [
        "//tests/go/kgo-verifier/pkg/util",
        "//tests/go/kgo-verifier/pkg/worker",
        "@com_github_google_uuid//:uuid",
        "@com_github_sirupsen_logrus//:logrus",
        "@com_github_twmb_franz_go//pkg/kgo",
        "@org_golang_x_time//rate",
    ],
)
