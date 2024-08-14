load("//bazel:build.bzl", "redpanda_cc_library")

redpanda_cc_library(
    name = "crc32",
    hdrs = [
        "crc32.h",
    ],
    include_prefix = "hashing",
    visibility = ["//visibility:public"],
    deps = [
        "@zlib",
    ],
)

redpanda_cc_library(
    name = "crc32c",
    hdrs = [
        "crc32c.h",
    ],
    include_prefix = "hashing",
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/bytes:iobuf",
        "@crc32c",
    ],
)

redpanda_cc_library(
    name = "secure",
    hdrs = [
        "secure.h",
    ],
    include_prefix = "hashing",
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/bytes",
        "//src/v/crypto",
    ],
)

redpanda_cc_library(
    name = "xx",
    hdrs = [
        "xx.h",
    ],
    defines = [
        "XXH_PRIVATE_API",
    ],
    include_prefix = "hashing",
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
        "murmur.h",
    ],
    copts = [
        "-Wno-implicit-fallthrough",
    ],
    include_prefix = "hashing",
    visibility = ["//visibility:public"],
)

redpanda_cc_library(
    name = "jump_consistent",
    hdrs = [
        "jump_consistent_hash.h",
    ],
    include_prefix = "hashing",
    visibility = ["//visibility:public"],
)

redpanda_cc_library(
    name = "combine",
    hdrs = [
        "combine.h",
    ],
    include_prefix = "hashing",
    visibility = ["//visibility:public"],
    deps = [
        "@abseil-cpp//absl/hash",
        "@boost//:functional",
    ],
)
