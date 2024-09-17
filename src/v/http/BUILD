load("//bazel:build.bzl", "redpanda_cc_library")

package(default_visibility = ["//visibility:public"])

redpanda_cc_library(
    name = "utils",
    srcs = [
        "utils.cc",
    ],
    hdrs = [
        "utils.h",
    ],
    include_prefix = "http",
    deps = [
        "//src/v/base",
        "//src/v/bytes",
        "//src/v/bytes:iobuf",
        "@abseil-cpp//absl/container:flat_hash_map",
        "@boost//:algorithm",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "http",
    srcs = [
        "chunk_encoding.cc",
        "client.cc",
        "iobuf_body.cc",
        "logger.cc",
    ],
    hdrs = [
        "chunk_encoding.h",
        "client.h",
        "iobuf_body.h",
        "logger.h",
        "probe.h",
    ],
    include_prefix = "http",
    deps = [
        ":utils",
        "//src/v/base",
        "//src/v/bytes",
        "//src/v/bytes:iobuf",
        "//src/v/bytes:scattered_message",
        "//src/v/config",
        "//src/v/net",
        "//src/v/ssx:sformat",
        "//src/v/utils:prefix_logger",
        "@boost//:asio",
        "@boost//:beast",
        "@boost//:optional",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "request_builder",
    srcs = [
        "request_builder.cc",
    ],
    hdrs = [
        "request_builder.h",
    ],
    include_prefix = "http",
    deps = [
        "//src/v/base",
        "//src/v/http:utils",
        "//src/v/thirdparty/ada",
        "@abseil-cpp//absl/container:flat_hash_map",
        "@abseil-cpp//absl/container:flat_hash_set",
        "@boost//:algorithm",
        "@boost//:beast",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "rest_entity",
    srcs = [
        "rest_client/rest_entity.cc",
    ],
    hdrs = [
        "rest_client/rest_entity.h",
    ],
    include_prefix = "http",
    deps = [
        "//src/v/base",
        "//src/v/http:request_builder",
        "@abseil-cpp//absl/container:flat_hash_map",
        "@boost//:beast",
        "@seastar",
    ],
)
