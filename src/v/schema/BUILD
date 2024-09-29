load("//bazel:build.bzl", "redpanda_cc_library")

package(
    default_visibility = [
        "//src/v/schema:__pkg__",
    ],
)

redpanda_cc_library(
    name = "registry",
    srcs = [
        "registry.cc",
    ],
    hdrs = [
        "registry.h",
    ],
    include_prefix = "schema",
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/base",
        "//src/v/pandaproxy",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "utils",
    srcs = [
        "utils.cc",
    ],
    hdrs = [
        "utils.h",
    ],
    include_prefix = "schema",
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/base",
        "//src/v/bytes:iobuf",
        "//src/v/bytes:iobuf_parser",
        "//src/v/pandaproxy",
        "@seastar",
    ],
)
