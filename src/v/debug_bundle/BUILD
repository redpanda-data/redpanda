load("//bazel:build.bzl", "redpanda_cc_library")

redpanda_cc_library(
    name = "debug_bundle",
    srcs = [
        "debug_bundle_service.cc",
    ],
    hdrs = [
        "debug_bundle_service.h",
        "fwd.h",
    ],
    implementation_deps = [
        ":metadata",
        ":probe",
        ":utils",
        "//src/v/bytes:iostream",
        "//src/v/container:fragmented_vector",
        "//src/v/ssx:future_util",
        "//src/v/utils:external_process",
        "@boost//:algorithm",
    ],
    include_prefix = "debug_bundle",
    visibility = ["//visibility:public"],
    deps = [
        ":error",
        ":types",
        "//src/v/base",
        "//src/v/config",
        "//src/v/storage",
        "//src/v/utils:mutex",
        "@fmt",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "error",
    srcs = [
        "error.cc",
    ],
    hdrs = [
        "error.h",
    ],
    include_prefix = "debug_bundle",
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/base",
    ],
)

redpanda_cc_library(
    name = "types",
    srcs = [
        "types.cc",
    ],
    hdrs = [
        "types.h",
    ],
    implementation_deps = [
        ":error",
        "//src/v/strings:string_switch",
        "@re2",
    ],
    include_prefix = "debug_bundle",
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/base",
        "//src/v/container:fragmented_vector",
        "//src/v/model",
        "//src/v/security",
        "//src/v/utils:named_type",
        "//src/v/utils:uuid",
        "@abseil-cpp//absl/container:btree",
        "@abseil-cpp//absl/strings",
        "@fmt",
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

redpanda_cc_library(
    name = "metadata",
    hdrs = [
        "metadata.h",
    ],
    implementation_deps = [
        "//src/v/serde:vector",
    ],
    include_prefix = "debug_bundle",
    visibility = ["//src/v/debug_bundle/tests:__subpackages__"],
    deps = [
        ":types",
        "//src/v/bytes",
        "//src/v/container:fragmented_vector",
        "//src/v/serde",
        "//src/v/serde:bytes",
        "//src/v/serde:named_type",
        "//src/v/serde:uuid",
        "//src/v/serde:variant",
        "//src/v/utils:uuid",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "utils",
    srcs = [
        "utils.cc",
    ],
    hdrs = [
        "utils.h",
    ],
    implementation_deps = [
        "//src/v/base",
        "//src/v/crypto",
        "//src/v/ssx:future_util",
        "@seastar",
    ],
    include_prefix = "debug_bundle",
    visibility = ["//src/v/debug_bundle/tests:__subpackages__"],
    deps = [
        "//src/v/bytes",
    ],
)

redpanda_cc_library(
    name = "probe",
    srcs = [
        "probe.cc",
    ],
    hdrs = [
        "probe.h",
    ],
    implementation_deps = [
        "//src/v/config",
        "@seastar",
    ],
    include_prefix = "debug_bundle",
    visibility = ["//visibility:private"],
    deps = [
        ":types",
        "//src/v/metrics",
    ],
)
