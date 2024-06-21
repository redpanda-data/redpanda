load("//bazel:build.bzl", "redpanda_cc_library")
load("//bazel:test.bzl", "redpanda_cc_btest")

redpanda_cc_library(
    name = "fast_prng",
    hdrs = [
        "include/random/fast_prng.h",
    ],
    strip_include_prefix = "include",
    visibility = ["//visibility:public"],
    deps = [
        "@abseil-cpp//absl/random",
    ],
)

redpanda_cc_library(
    name = "generators",
    srcs = [
        "generators.cc",
    ],
    hdrs = [
        "include/random/generators.h",
    ],
    strip_include_prefix = "include",
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/base",
        "//src/v/crypto",
    ],
)

redpanda_cc_library(
    name = "time_jitter",
    hdrs = [
        "include/random/simple_time_jitter.h",
    ],
    strip_include_prefix = "include",
    visibility = ["//visibility:public"],
    deps = [
        ":fast_prng",
    ],
)

redpanda_cc_btest(
    name = "random_test",
    timeout = "short",
    srcs = [
        "tests/random_test.cc",
    ],
    deps = [
        ":fast_prng",
        ":generators",
        "//src/v/base",
        "//src/v/test_utils:seastar_boost",
    ],
)
