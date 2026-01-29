load("//bazel:build.bzl", "redpanda_cc_library")

package(default_visibility = ["//src/v/lsm:__subpackages__"])

redpanda_cc_library(
    name = "logger",
    srcs = ["logger.cc"],
    hdrs = ["logger.h"],
    deps = ["@seastar"],
)

redpanda_cc_library(
    name = "iterator",
    srcs = ["iterator.cc"],
    hdrs = [
        "iterator.h",
    ],
    implementation_deps = [
        "//src/v/lsm/core:exceptions",
    ],
    deps = [
        ":keys",
        "//src/v/base",
        "//src/v/bytes:iobuf",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "two_level_iterator",
    srcs = ["two_level_iterator.cc"],
    hdrs = ["two_level_iterator.h"],
    implementation_deps = [
        "//src/v/lsm/core:exceptions",
    ],
    deps = [
        ":iterator",
        "//src/v/base",
        "//src/v/bytes:iobuf",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "merging_iterator",
    srcs = ["merging_iterator.cc"],
    hdrs = ["merging_iterator.h"],
    deps = [
        ":iterator",
        "//src/v/base",
        "//src/v/bytes:iobuf",
        "//src/v/container:chunked_vector",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "keys",
    srcs = ["keys.cc"],
    hdrs = ["keys.h"],
    implementation_deps = [
        "@abseil-cpp//absl/strings",
    ],
    deps = [
        "//src/v/base",
        "//src/v/bytes:iobuf",
        "//src/v/lsm/core:keys",
        "//src/v/utils:named_type",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "files",
    srcs = ["files.cc"],
    hdrs = ["files.h"],
    implementation_deps = [
        "@abseil-cpp//absl/strings",
    ],
    deps = [
        "//src/v/base",
        "//src/v/utils:named_type",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "options",
    srcs = ["options.cc"],
    hdrs = ["options.h"],
    deps = [
        ":files",
        "//src/v/base",
        "//src/v/lsm/core:compression",
        "@abseil-cpp//absl/time",
        "@seastar",
    ],
)
