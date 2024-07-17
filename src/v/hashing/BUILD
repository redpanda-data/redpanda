load("//bazel:build.bzl", "redpanda_cc_library")

redpanda_cc_library(
    name = "crc32c",
    hdrs = [
        "include/hashing/crc32c.h",
    ],
    strip_include_prefix = "include",
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/bytes:iobuf",
        "@crc32c",
    ],
)

redpanda_cc_library(
    name = "secure",
    hdrs = [
        "include/hashing/secure.h",
    ],
    strip_include_prefix = "include",
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/bytes",
        "//src/v/crypto",
    ],
)

redpanda_cc_library(
    name = "xx",
    hdrs = [
        "include/hashing/xx.h",
    ],
    defines = [
        "XXH_PRIVATE_API",
    ],
    strip_include_prefix = "include",
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/base",
        "@xxhash",
    ],
)
