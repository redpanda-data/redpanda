load("@rules_go//go:def.bzl", "go_binary", "go_library")

go_binary(
    name = "kafka_request_generator",
    embed = [":kreq-gen_lib"],
    visibility = ["//visibility:public"],
)

go_library(
    name = "kreq-gen_lib",
    srcs = ["kafka-request-generator.go"],
    importpath = "github.com/redpanda-data/redpanda/src/v/test_utils/go/kreq-gen",
    visibility = ["//visibility:private"],
    deps = ["@com_github_twmb_franz_go_pkg_kmsg//:kmsg"],
)
