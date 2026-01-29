load("//bazel:build.bzl", "redpanda_cc_library")

redpanda_cc_library(
    name = "registry",
    hdrs = [
        "registry.h",
        "registry_def.h",
    ],
    visibility = [
        "//src/v/cloud_storage_clients:__pkg__",
        "//src/v/cloud_storage_clients/detail/tests:__pkg__",
    ],
    deps = [
        "//src/v/base",
        "//src/v/random:time_jitter",
        "//src/v/ssx:future_util",
        "//src/v/ssx:mutex",
        "//src/v/ssx:semaphore",
        "@seastar",
    ],
)
