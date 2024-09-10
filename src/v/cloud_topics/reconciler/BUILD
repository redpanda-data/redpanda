load("//bazel:build.bzl", "redpanda_cc_library")

redpanda_cc_library(
    name = "range_batch_consumer",
    srcs = [
        "range_batch_consumer.cc",
    ],
    hdrs = [
        "range_batch_consumer.h",
    ],
    include_prefix = "cloud_topics/reconciler",
    deps = [
        "//src/v/base",
        "//src/v/bytes:iobuf",
        "//src/v/model",
        "@seastar",
    ],
)
