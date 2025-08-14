load("//bazel:build.bzl", "redpanda_cc_library")

redpanda_cc_library(
    name = "string_switch_table",
    hdrs = ["string_switch_table.h"],
    visibility = ["//src/v/iceberg/conversion/json_schema:__subpackages__"],
    deps = [
        "//src/v/strings:string_switch",
    ],
)
