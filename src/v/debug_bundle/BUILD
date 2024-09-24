load("//bazel:build.bzl", "redpanda_cc_library")

redpanda_cc_library(
    name = "debug_bundle",
    srcs = [
        "debug_bundle_service.cc",
        "error.cc",
        "types.cc",
    ],
    hdrs = [
        "debug_bundle_service.h",
        "error.h",
        "fwd.h",
        "types.h",
    ],
    implementation_deps = [
        "//src/v/ssx:future_util",
        "//src/v/strings:string_switch",
        "//src/v/utils:external_process",
        "@boost//:algorithm",
    ],
    include_prefix = "debug_bundle",
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/base",
        "//src/v/config",
        "//src/v/container:fragmented_vector",
        "//src/v/model",
        "//src/v/security",
        "//src/v/utils:mutex",
        "//src/v/utils:named_type",
        "//src/v/utils:uuid",
        "@abseil-cpp//absl/container:btree",
        "@abseil-cpp//absl/strings",
        "@fmt",
        "@re2",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "json",
    hdrs = [
        "json.h",
    ],
    include_prefix = "debug_bundle",
    visibility = ["//visibility:public"],
    deps = [
        ":debug_bundle",
        "//src/v/base",
        "//src/v/json",
        "//src/v/reflection:type_traits",
    ],
)
