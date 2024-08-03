load("//bazel:build.bzl", "redpanda_cc_library")

redpanda_cc_library(
    name = "crc32",
    hdrs = [
        "include/hashing/crc32.h",
    ],
    strip_include_prefix = "include",
    visibility = ["//visibility:public"],
    deps = [
        "@zlib",
    ],
)

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

redpanda_cc_library(
    name = "murmur",
    srcs = [
        "murmur.cc",
    ],
    hdrs = [
        "include/hashing/murmur.h",
    ],
    copts = [
        "-Wno-implicit-fallthrough",
    ],
    strip_include_prefix = "include",
    visibility = ["//visibility:public"],
)

redpanda_cc_library(
    name = "jump_consistent",
    hdrs = [
        "include/hashing/jump_consistent_hash.h",
    ],
    strip_include_prefix = "include",
    visibility = ["//visibility:public"],
)

redpanda_cc_library(
    name = "combine",
    hdrs = [
        "include/hashing/combine.h",
    ],
    strip_include_prefix = "include",
    visibility = ["//visibility:public"],
    deps = [
        "@abseil-cpp//absl/hash",
        "@boost//:functional",
    ],
)
