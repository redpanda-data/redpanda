load("//bazel:build.bzl", "redpanda_cc_library")

redpanda_cc_library(
    name = "txn_reader",
    srcs = [
        "txn_reader.cc",
    ],
    hdrs = [
        "txn_reader.h",
    ],
    implementation_deps = [
        "//src/v/kafka/data:partition_proxy",
        "//src/v/storage",
        "@abseil-cpp//absl/container:btree",
        "@fmt",
    ],
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/model",
        "@seastar",
    ],
)
