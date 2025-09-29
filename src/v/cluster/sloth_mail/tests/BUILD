load("//bazel:test.bzl", "redpanda_test_cc_library")

redpanda_test_cc_library(
    name = "kinds",
    hdrs = [
        "kinds.h",
    ],
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/base",
        "//src/v/cluster/sloth_mail/impl",
        "//src/v/container:chunked_vector",
        "//src/v/serde",
        "@seastar",
    ],
)
