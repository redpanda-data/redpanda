load("//bazel:build.bzl", "redpanda_cc_library")

package(
    default_visibility = ["//src/v/lsm:__subpackages__"],
)

redpanda_cc_library(
    name = "builder",
    srcs = [
        "builder.cc",
    ],
    hdrs = [
        "builder.h",
    ],
    implementation_deps = [
        ":footer",
        "//src/v/hashing:crc32c",
    ],
    deps = [
        "//src/v/base",
        "//src/v/bytes:iobuf",
        "//src/v/lsm/block:builder",
        "//src/v/lsm/block:filter",
        "//src/v/lsm/block:handle",
        "//src/v/lsm/core:compression",
        "//src/v/lsm/core/internal:keys",
        "//src/v/lsm/core/internal:options",
        "//src/v/lsm/io:persistence",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "footer",
    srcs = [
        "footer.cc",
    ],
    hdrs = [
        "footer.h",
    ],
    implementation_deps = [
        "//src/v/base",
        "//src/v/bytes:iobuf_parser",
        "//src/v/hashing:crc32c",
        "//src/v/lsm/core:exceptions",
    ],
    deps = [
        "//src/v/bytes:iobuf",
        "//src/v/lsm/block:handle",
    ],
)

redpanda_cc_library(
    name = "reader",
    srcs = [
        "reader.cc",
    ],
    hdrs = [
        "reader.h",
    ],
    implementation_deps = [
        ":footer",
        "//src/v/bytes:iobuf",
        "//src/v/hashing:crc32c",
        "//src/v/lsm/block:contents",
        "//src/v/lsm/block:filter",
        "//src/v/lsm/block:handle",
        "//src/v/lsm/block:reader",
        "//src/v/lsm/core:compression",
        "//src/v/lsm/core:exceptions",
        "//src/v/lsm/core/internal:two_level_iterator",
        "//src/v/lsm/io:readahead_file_reader",
    ],
    deps = [
        ":block_cache",
        "//src/v/base",
        "//src/v/lsm/core/internal:iterator",
        "//src/v/lsm/core/internal:keys",
        "//src/v/lsm/io:persistence",
        "@abseil-cpp//absl/functional:function_ref",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "block_cache",
    srcs = [
        "block_cache.cc",
    ],
    hdrs = [
        "block_cache.h",
    ],
    implementation_deps = [
        "//src/v/container:chunked_hash_map",
        "//src/v/ssx:semaphore",
        "//src/v/utils:chunked_kv_cache",
    ],
    deps = [
        "//src/v/base",
        "//src/v/lsm/block:handle",
        "//src/v/lsm/block:reader",
        "//src/v/lsm/core:probe",
        "//src/v/lsm/core/internal:files",
        "@seastar",
    ],
)
