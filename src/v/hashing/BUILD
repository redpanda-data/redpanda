load("//bazel:build.bzl", "redpanda_cc_library")

redpanda_cc_library(
    name = "crc32",
    hdrs = [
        "crc32.h",
    ],
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
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/bytes:iobuf",
    ],
)

redpanda_cc_library(
    name = "jump_consistent",
    hdrs = [
        "jump_consistent_hash.h",
    ],
    visibility = ["//visibility:public"],
)

redpanda_cc_library(
    name = "combine",
    hdrs = [
        "combine.h",
    ],
    visibility = ["//visibility:public"],
    deps = [
        "@abseil-cpp//absl/hash",
        "@boost//:functional",
    ],
)
