load("//bazel:build.bzl", "redpanda_cc_library")

redpanda_cc_library(
    name = "crash_tracker",
    srcs = [
        "api.cc",
        "logger.cc",
        "service.cc",
    ],
    hdrs = [
        "api.h",
        "logger.h",
        "service.h",
        "types.h",
    ],
    implementation_deps = [
        "//src/v/bytes",
        "//src/v/bytes:iobuf",
        "//src/v/config",
        "//src/v/hashing:xx",
        "//src/v/model",
        "//src/v/serde",
        "//src/v/serde:bytes",
        "//src/v/serde:iobuf",
        "//src/v/serde:sstring",
        "//src/v/serde:vector",
        "//src/v/utils:file_io",
        "@fmt",
    ],
    include_prefix = "crash_tracker",
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/base",
        "//src/v/model",
        "//src/v/serde",
        "@seastar",
    ],
)
