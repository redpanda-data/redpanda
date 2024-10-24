load("//bazel:build.bzl", "redpanda_cc_library")

redpanda_cc_library(
    name = "iobuf",
    srcs = [
        "iobuf.cc",
    ],
    hdrs = [
        "details/io_allocation_size.h",
        "details/io_byte_iterator.h",
        "details/io_fragment.h",
        "details/io_iterator_consumer.h",
        "details/io_placeholder.h",
        "details/out_of_range.h",
        "iobuf.h",
    ],
    include_prefix = "bytes",
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
        "bytes.h",
    ],
    include_prefix = "bytes",
    visibility = ["//visibility:public"],
    deps = [
        ":iobuf",
        "//src/v/base",
        "@abseil-cpp//absl/container:inlined_vector",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "hash",
    hdrs = [
        "hash.h",
    ],
    include_prefix = "bytes",
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
        "random.h",
    ],
    include_prefix = "bytes",
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
        "iostream.h",
    ],
    include_prefix = "bytes",
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
        "scattered_message.h",
    ],
    include_prefix = "bytes",
    visibility = ["//visibility:public"],
    deps = [
        ":iobuf",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "iobuf_parser",
    hdrs = [
        "iobuf_parser.h",
    ],
    include_prefix = "bytes",
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
        "streambuf.h",
    ],
    include_prefix = "bytes",
    visibility = ["//visibility:public"],
    deps = [
        ":iobuf",
    ],
)
