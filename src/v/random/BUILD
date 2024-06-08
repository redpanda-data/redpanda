load("//bazel:build.bzl", "redpanda_cc_library")

redpanda_cc_library(
    name = "random",
    srcs = [
        "generators.cc",
    ],
    hdrs = [
        "include/random/fast_prng.h",
        "include/random/generators.h",
        "include/random/simple_time_jitter.h",
    ],
    strip_include_prefix = "include",
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/base",
        "//src/v/crypto",
    ],
)
