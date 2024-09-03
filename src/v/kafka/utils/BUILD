load("//bazel:build.bzl", "redpanda_cc_library")

redpanda_cc_library(
    name = "txn_reader",
    srcs = [
        "txn_reader.cc",
    ],
    hdrs = [
        "txn_reader.h",
    ],
    include_prefix = "kafka/utils",
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/kafka/server",
        "//src/v/model",
        "//src/v/ssx:future_util",
        "//src/v/ssx:sformat",
        "//src/v/storage",
        "@abseil-cpp//absl/container:btree",
        "@fmt",
        "@seastar",
    ],
)
