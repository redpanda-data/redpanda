load("//bazel:build.bzl", "redpanda_cc_library")
load("//bazel:test.bzl", "redpanda_test_cc_library")

redpanda_cc_library(
    name = "generators",
    srcs = [
        "generators.cc",
    ],
    hdrs = [
        "generators.h",
    ],
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/base",
        "@abseil-cpp//absl/random",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "secure_random",
    srcs = [
        "secure_generators.cc",
    ],
    hdrs = [
        "secure_generators.h",
    ],
    implementation_deps = [
        ":generators",
        "//src/v/crypto",
    ],
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/base",
        "@seastar",
    ],
)

redpanda_test_cc_library(
    name = "test_seeding",
    srcs = [
        "test_seeding.cc",
    ],
    hdrs = [
        "test_seeding.h",
    ],
    implementation_deps = [
        ":generators",
    ],
    visibility = ["//visibility:public"],
    deps = [
    ],
)

redpanda_cc_library(
    name = "time_jitter",
    hdrs = [
        "simple_time_jitter.h",
    ],
    visibility = ["//visibility:public"],
    deps = [
        ":generators",
    ],
)
