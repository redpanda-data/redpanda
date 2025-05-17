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
        "deps.h",
        "manager.h",
    ],
    implementation_deps = [
        ":logger",
    ],
    include_prefix = "panda_link",
    visibility = ["//src/v/panda_link:__subpackages__"],
    deps = [
        "//src/v/container:fragmented_vector",
        "//src/v/kafka/client",
        "//src/v/model",
        "//src/v/ssx:work_queue",
    ],
)

redpanda_cc_library(
    name = "fwd",
    hdrs = [
        "fwd.h",
    ],
    include_prefix = "panda_link",
    visibility = ["//visibility:public"],
)

redpanda_cc_library(
    name = "panda_link",
    srcs = [
        "api.cc",
        "errc.cc",
    ],
    hdrs = [
        "api.h",
        "errc.h",
    ],
    implementation_deps = [
        ":impl",
        ":logger",
    ],
    include_prefix = "panda_link",
    visibility = ["//visibility:public"],
    deps = [
        ":fwd",
        "//src/v/base",
        "//src/v/cluster",
        "//src/v/model",
        "//src/v/raft",
        "//src/v/ssx:work_queue",
        "@seastar",
    ],
)
