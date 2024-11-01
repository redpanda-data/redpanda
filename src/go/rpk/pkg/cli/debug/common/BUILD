load("@rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "common",
    srcs = ["common.go"],
    importpath = "github.com/redpanda-data/redpanda/src/go/rpk/pkg/cli/debug/common",
    visibility = ["//visibility:public"],
    deps = ["@com_github_spf13_pflag//:pflag"],
)

go_test(
    name = "common_test",
    srcs = ["common_test.go"],
    embed = [":common"],
    deps = ["@com_github_stretchr_testify//require"],
)
