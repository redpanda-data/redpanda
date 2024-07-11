load("//bazel:build.bzl", "redpanda_cc_library")

redpanda_cc_library(
    name = "iobuf",
    srcs = [
        "iobuf.cc",
    ],
    hdrs = [
        "include/bytes/details/io_allocation_size.h",
        "include/bytes/details/io_byte_iterator.h",
        "include/bytes/details/io_fragment.h",
        "include/bytes/details/io_iterator_consumer.h",
        "include/bytes/details/io_placeholder.h",
        "include/bytes/details/out_of_range.h",
        "include/bytes/iobuf.h",
    ],
    strip_include_prefix = "include",
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/base",
        "//src/v/container:intrusive",
        "@fmt",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "bytes",
    srcs = [
        "bytes.cc",
    ],
    hdrs = [
        "include/bytes/bytes.h",
    ],
    strip_include_prefix = "include",
    visibility = ["//visibility:public"],
    deps = [
        ":iobuf",
        "//src/v/base",
        "@abseil-cpp//absl/hash",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "hash",
    hdrs = [
        "include/bytes/hash.h",
    ],
    strip_include_prefix = "include",
    visibility = ["//visibility:public"],
    deps = [
        ":iobuf",
        "@boost//:container_hash",
    ],
)

redpanda_cc_library(
    name = "random",
    srcs = [
        "random.cc",
    ],
    hdrs = [
        "include/bytes/random.h",
    ],
    strip_include_prefix = "include",
    visibility = ["//visibility:public"],
    deps = [
        ":bytes",
        ":iobuf",
        "//src/v/random:generators",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "iostream",
    srcs = [
        "iostream.cc",
    ],
    hdrs = [
        "include/bytes/iostream.h",
    ],
    strip_include_prefix = "include",
    visibility = ["//visibility:public"],
    deps = [
        ":iobuf",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "scattered_message",
    srcs = [
        "scattered_message.cc",
    ],
    hdrs = [
        "include/bytes/scattered_message.h",
    ],
    strip_include_prefix = "include",
    visibility = ["//visibility:public"],
    deps = [
        ":iobuf",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "iobuf_parser",
    hdrs = [
        "include/bytes/iobuf_parser.h",
    ],
    strip_include_prefix = "include",
    visibility = ["//visibility:public"],
    deps = [
        ":bytes",
        "//src/v/strings:utf8",
        "//src/v/utils:vint",
    ],
)

redpanda_cc_library(
    name = "streambuf",
    hdrs = [
        "include/bytes/streambuf.h",
    ],
    strip_include_prefix = "include",
    visibility = ["//visibility:public"],
    deps = [
        ":iobuf",
    ],
)
