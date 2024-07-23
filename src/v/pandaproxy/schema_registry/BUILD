load("//bazel:build.bzl", "redpanda_cc_library")

redpanda_cc_library(
    name = "config",
    hdrs = [
        "schema_id_validation.h",
    ],
    include_prefix = "pandaproxy/schema_registry",
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/strings:string_switch",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "subject_name_strategy",
    hdrs = [
        "subject_name_strategy.h",
    ],
    include_prefix = "pandaproxy/schema_registry",
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/strings:string_switch",
    ],
)
