load("//bazel:test.bzl", "redpanda_test_cc_library")

redpanda_test_cc_library(
    name = "fake_partition_proxy",
    hdrs = [
        "fake_partition_proxy_impl.h",
        "fake_partition_proxy_source.h",
    ],
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/container:chunked_vector",
        "//src/v/kafka/data:partition_proxy",
        "//src/v/kafka/data:partition_proxy_source",
        "//src/v/model",
    ],
)
