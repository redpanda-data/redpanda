load("//bazel:build.bzl", "redpanda_cc_library")

redpanda_cc_library(
    name = "string",
    srcs = [
        "string.cc",
    ],
    hdrs = [
        "string.h",
    ],
    visibility = [
        ":__subpackages__",
        "//src/v/serde/json:__pkg__",
    ],
    deps = [
        "//src/v/base",
        "//src/v/bytes:iobuf",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "numeric",
    srcs = [
        "numeric.cc",
    ],
    hdrs = [
        "numeric.h",
        "pow10.h",
        "strod.h",
    ],
    visibility = [
        ":__subpackages__",
        "//src/v/serde/json:__pkg__",
    ],
    deps = [
        "//src/v/base",
        "@seastar",
    ],
)
