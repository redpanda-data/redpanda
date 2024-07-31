load("@rules_go//go:def.bzl", "go_binary")

go_binary(
    name = "kafka_request_generator",
    srcs = ["kafka-request-generator.go"],
    visibility = ["//visibility:public"],
    deps = [
        "@com_github_twmb_franz_go_pkg_kmsg//:kmsg",
    ],
)
