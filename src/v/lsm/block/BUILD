load("//bazel:build.bzl", "redpanda_cc_library")

package(
    default_visibility = ["//src/v/lsm:__subpackages__"],
)

redpanda_cc_library(
    name = "builder",
    srcs = [
        "builder.cc",
    ],
    hdrs = [
        "builder.h",
    ],
    deps = [
        "//src/v/base",
        "//src/v/bytes:iobuf",
        "//src/v/container:chunked_vector",
        "//src/v/lsm/core/internal:keys",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "handle",
    srcs = ["handle.cc"],
    hdrs = [
        "handle.h",
    ],
    implementation_deps = [
        "//src/v/base",
        "//src/v/bytes:iobuf_parser",
    ],
    deps = [
        "//src/v/bytes:iobuf",
    ],
)

redpanda_cc_library(
    name = "contents",
    srcs = [
        "contents.cc",
    ],
    hdrs = [
        "contents.h",
    ],
    implementation_deps = [
        "//src/v/bytes:iobuf_parser",
    ],
    deps = [
        ":handle",
        "//src/v/base",
        "//src/v/bytes:ioarray",
        "//src/v/bytes:iobuf",
        "//src/v/lsm/io:persistence",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "filter",
    srcs = [
        "filter.cc",
    ],
    hdrs = [
        "filter.h",
    ],
    implementation_deps = [
        "//src/v/hashing:xx",
    ],
    deps = [
        ":contents",
        "//src/v/base",
        "//src/v/bytes:iobuf",
        "//src/v/container:chunked_vector",
        "//src/v/lsm/core/internal:keys",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "reader",
    srcs = [
        "reader.cc",
    ],
    hdrs = [
        "reader.h",
    ],
    implementation_deps = [
        "//src/v/lsm/core:exceptions",
    ],
    deps = [
        ":contents",
        "//src/v/base",
        "//src/v/lsm/core/internal:iterator",
        "//src/v/lsm/core/internal:keys",
        "@seastar",
    ],
)
