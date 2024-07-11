load("//bazel:build.bzl", "redpanda_cc_library")

redpanda_cc_library(
    name = "vint",
    srcs = [
        "vint.cc",
    ],
    hdrs = [
        "vint.h",
        "vint_iostream.h",
    ],
    include_prefix = "utils",
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/bytes",
        "//src/v/bytes:iostream",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "functional",
    hdrs = [
        "functional.h",
    ],
    include_prefix = "utils",
    visibility = ["//visibility:public"],
)

redpanda_cc_library(
    name = "inet_address",
    hdrs = [
        "inet_address_wrapper.h",
    ],
    include_prefix = "utils",
    visibility = ["//visibility:public"],
)

redpanda_cc_library(
    name = "tristate",
    hdrs = [
        "tristate.h",
    ],
    include_prefix = "utils",
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/base",
    ],
)

redpanda_cc_library(
    name = "memory_data_source",
    hdrs = [
        "memory_data_source.h",
    ],
    include_prefix = "utils",
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/base",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "mutex",
    hdrs = [
        "mutex.h",
    ],
    include_prefix = "utils",
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/base",
        "//src/v/ssx:semaphore",
    ],
)

redpanda_cc_library(
    name = "static_deleter_fn",
    hdrs = [
        "static_deleter_fn.h",
    ],
    include_prefix = "utils",
    visibility = ["//visibility:public"],
)

redpanda_cc_library(
    name = "named_type",
    hdrs = [
        "named_type.h",
    ],
    include_prefix = "utils",
    visibility = ["//visibility:public"],
    deps = [
        "@fmt",
    ],
)

redpanda_cc_library(
    name = "to_string",
    hdrs = [
        "to_string.h",
    ],
    include_prefix = "utils",
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/base",
        "@fmt",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "object_pool",
    hdrs = [
        "object_pool.h",
    ],
    include_prefix = "utils",
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/base",
    ],
)

redpanda_cc_library(
    name = "human",
    srcs = [
        "human.cc",
    ],
    hdrs = [
        "human.h",
    ],
    include_prefix = "utils",
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/base",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "unresolved_address",
    hdrs = [
        "unresolved_address.h",
    ],
    include_prefix = "utils",
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/serde",
    ],
)

redpanda_cc_library(
    name = "hdr_hist",
    srcs = [
        "hdr_hist.cc",
    ],
    hdrs = [
        "hdr_hist.h",
    ],
    exclude_layering_check = True,
    include_prefix = "utils",
    visibility = ["//visibility:public"],
    deps = [
        ":human",
        ":static_deleter_fn",
        "//src/v/base",
        "@boost//:intrusive",
        "@fmt",
        "@hdrhistogram",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "log_hist",
    srcs = [
        "log_hist.cc",
    ],
    hdrs = [
        "log_hist.h",
    ],
    exclude_layering_check = True,
    include_prefix = "utils",
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/container:fragmented_vector",
        "//src/v/reflection:adl",
    ],
)

redpanda_cc_library(
    name = "uuid",
    srcs = [
        "uuid.cc",
    ],
    hdrs = [
        "uuid.h",
    ],
    include_prefix = "utils",
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/base",
        "//src/v/bytes",
        "//src/v/bytes:iobuf",
        "@abseil-cpp//absl/hash",
        "@boost//:uuid",
        "@fmt",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "xid",
    srcs = [
        "xid.cc",
    ],
    hdrs = [
        "xid.h",
    ],
    include_prefix = "utils",
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/base",
        "//src/v/bytes:iobuf_parser",
        "//src/v/serde",
        "//src/v/serde:array",
        "//src/v/ssx:sformat",
        "@fmt",
        "@seastar",
    ],
)
