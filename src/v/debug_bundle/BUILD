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
        "//src/v/strings:string_switch",
    ],
    include_prefix = "debug_bundle",
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/base",
        "//src/v/config",
        "//src/v/container:fragmented_vector",
        "//src/v/model",
        "//src/v/security",
        "//src/v/utils:uuid",
        "@abseil-cpp//absl/container:btree",
        "@fmt",
        "@seastar",
    ],
)
