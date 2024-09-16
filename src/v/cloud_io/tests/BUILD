load("//bazel:test.bzl", "redpanda_test_cc_library")

redpanda_test_cc_library(
    name = "scoped_remote",
    srcs = [
        "scoped_remote.cc",
    ],
    hdrs = [
        "scoped_remote.h",
    ],
    include_prefix = "cloud_io/tests",
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/cloud_io:remote",
        "//src/v/cloud_storage_clients",
        "@seastar",
    ],
)
