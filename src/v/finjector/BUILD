load("//bazel:build.bzl", "redpanda_cc_library")

redpanda_cc_library(
    name = "finjector",
    srcs = [
        "hbadger.cc",
        "stress_fiber.cc",
    ],
    hdrs = [
        "hbadger.h",
        "stress_fiber.h",
    ],
    include_prefix = "finjector",
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/base",
        "//src/v/random:generators",
        "//src/v/ssx:future_util",
        "@abseil-cpp//absl/container:node_hash_map",
        "@seastar",
    ],
)
