load("@rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "util",
    srcs = ["util.go"],
    importpath = "github.com/redpanda-data/redpanda/tests/go/kgo-verifier/pkg/util",
    visibility = ["//visibility:public"],
    deps = ["@com_github_sirupsen_logrus//:logrus"],
)

go_test(
    name = "util_test",
    srcs = ["util_test.go"],
    deps = [":util"],
)
