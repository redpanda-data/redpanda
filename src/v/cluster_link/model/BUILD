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
    implementation_deps = [
        "//src/v/ssx:async_algorithm",
        "//src/v/utils:to_string",
    ],
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/base",
        "//src/v/cluster_link:errc",
        "//src/v/container:chunked_hash_map",
        "//src/v/kafka/protocol",
        "//src/v/model",
        "//src/v/serde",
        "//src/v/serde:bool_class",
        "//src/v/serde:chrono",
        "//src/v/serde:enum",
        "//src/v/serde:map",
        "//src/v/serde:named_type",
        "//src/v/serde:optional",
        "//src/v/serde:variant",
        "//src/v/serde:vector",
        "//src/v/utils:absl_sstring_hash",
        "//src/v/utils:named_type",
        "//src/v/utils:unresolved_address",
        "//src/v/utils:uuid",
        "@abseil-cpp//absl/container:flat_hash_set",
        "@fmt",
        "@seastar",
    ],
)
