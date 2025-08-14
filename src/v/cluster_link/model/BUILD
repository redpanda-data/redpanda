load("//bazel:build.bzl", "redpanda_cc_library")

redpanda_cc_library(
    name = "model",
    srcs = [
        "filter_utils.cc",
        "types.cc",
    ],
    hdrs = [
        "filter_utils.h",
        "types.h",
    ],
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/container:chunked_hash_map",
        "//src/v/model",
        "//src/v/serde",
        "//src/v/serde:variant",
        "//src/v/utils:named_type",
        "//src/v/utils:unresolved_address",
        "//src/v/utils:uuid",
        "@abseil-cpp//absl/container:flat_hash_set",
        "@fmt",
        "@seastar",
    ],
)
