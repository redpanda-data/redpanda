load("//bazel:build.bzl", "redpanda_cc_library")
load("//bazel:test.bzl", "redpanda_cc_btest")

redpanda_cc_library(
    name = "json",
    srcs = [
        "json.cc",
    ],
    hdrs = [
        "_include_first.h",
        "allocator.h",
        "document.h",
        "encodings.h",
        "istreamwrapper.h",
        "json.h",
        "logger.h",
        "ostreamwrapper.h",
        "pointer.h",
        "prettywriter.h",
        "reader.h",
        "schema.h",
        "stream.h",
        "stringbuffer.h",
        "types.h",
        "validator.h",
        "writer.h",
    ],
    include_prefix = "json",
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/base",
        "//src/v/serde",
        "//src/v/utils:named_type",
        "//src/v/utils:unresolved_address",
        "@fmt",
        "@rapidjson",
        "@seastar",
    ],
)

redpanda_cc_btest(
    name = "json_serialization_test",
    timeout = "short",
    srcs = [
        "tests/json_serialization_test.cc",
    ],
    deps = [
        ":json",
        "//src/v/base",
        "//src/v/test_utils:seastar_boost",
        "@seastar",
        "@seastar//:testing",
    ],
)
