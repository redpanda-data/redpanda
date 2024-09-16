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
        "//src/v/kafka/server",
        "//src/v/storage",
        "@abseil-cpp//absl/container:btree",
        "@fmt",
    ],
    include_prefix = "kafka/utils",
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/model",
        "@seastar",
    ],
)
