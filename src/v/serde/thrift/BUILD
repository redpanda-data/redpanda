load("//bazel:build.bzl", "redpanda_cc_library")

package(
    default_visibility = [
        "//src/v/serde/parquet:__subpackages__",
        "//src/v/serde/thrift:__subpackages__",
    ],
)

redpanda_cc_library(
    name = "compact",
    srcs = [
        "compact.cc",
    ],
    hdrs = [
        "compact.h",
    ],
    include_prefix = "serde/thrift",
    deps = [
        "//src/v/base",
        "//src/v/bytes",
        "//src/v/bytes:iobuf",
        "//src/v/utils:vint",
        "@seastar",
    ],
)
