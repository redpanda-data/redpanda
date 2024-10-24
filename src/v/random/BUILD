load("//bazel:build.bzl", "redpanda_cc_library")

redpanda_cc_library(
    name = "fast_prng",
    hdrs = [
        "fast_prng.h",
    ],
    include_prefix = "random",
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
        "generators.h",
    ],
    include_prefix = "random",
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
        "simple_time_jitter.h",
    ],
    include_prefix = "random",
    visibility = ["//visibility:public"],
    deps = [
        ":fast_prng",
    ],
)
