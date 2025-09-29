load("//bazel:test.bzl", "redpanda_cc_gtest", "redpanda_test_cc_library")

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

redpanda_test_cc_library(
    name = "backend_fixture",
    srcs = [
        "backend_fixture.cc",
    ],
    hdrs = [
        "backend_fixture.h",
        "config.h",
    ],
    visibility = ["//visibility:public"],
    deps = [
        ":kinds",
        "//src/v/base",
        "//src/v/cluster",
        "//src/v/cluster/sloth_mail/impl",
        "//src/v/container:chunked_vector",
        "//src/v/serde",
        "//src/v/test_utils:gtest",
        "@googletest//:gtest",
        "@seastar",
    ],
)

redpanda_cc_gtest(
    name = "backend_test",
    timeout = "short",
    srcs = [
        "backend_test.cc",
    ],
    deps = [
        ":backend_fixture",
        "//src/v/container:chunked_vector",
        "//src/v/model",
        "//src/v/test_utils:gtest",
        "@googletest//:gtest",
        "@seastar",
    ],
)
