load("//bazel:build.bzl", "redpanda_cc_library")

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
        "@seastar",
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
