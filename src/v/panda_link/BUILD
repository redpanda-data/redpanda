load("//bazel:build.bzl", "redpanda_cc_library")

redpanda_cc_library(
    name = "logger",
    srcs = [
        "logger.cc",
    ],
    hdrs = [
        "logger.h",
    ],
    include_prefix = "panda_link",
    visibility = ["//src/v/panda_link:__subpackages__"],
    deps = [
        "//src/v/base",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "impl",
    srcs = [
        "manager.cc",
    ],
    hdrs = [
        "manager.h",
    ],
    implementation_deps = [
        ":logger",
    ],
    include_prefix = "panda_link",
    visibility = ["//src/v/panda_link:__subpackages__"],
    deps = [
        "//src/v/model",
        "//src/v/ssx:work_queue",
    ],
)
