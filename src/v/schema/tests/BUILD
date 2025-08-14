load("//bazel:test.bzl", "redpanda_test_cc_library")

redpanda_test_cc_library(
    name = "fake_registry",
    srcs = [
        "fake_registry.cc",
    ],
    hdrs = [
        "fake_registry.h",
    ],
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/base",
        "//src/v/pandaproxy/schema_registry:types",
        "//src/v/schema:registry",
        "@seastar",
    ],
)
