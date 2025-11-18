load("@bazel_skylib//rules:common_settings.bzl", "bool_flag")
load("@rules_python//python:defs.bzl", "py_binary")

bool_flag(
    name = "gofips",
    build_setting_default = False,
)

config_setting(
    name = "gofips_enabled",
    flag_values = {
        ":gofips": "true",
    },
)

bool_flag(
    name = "has_sanitizers",
    build_setting_default = False,
)

config_setting(
    name = "enable_fuzz_testing",
    flag_values = {
        ":has_sanitizers": "true",
    },
)

config_setting(
    name = "optimized_build",
    values = {
        "compilation_mode": "opt",
    },
)

filegroup(
    name = "bench_wrapper",
    srcs = ["bench_wrapper.py"],
    visibility = ["//visibility:public"],
)

py_binary(
    name = "split_swagger_json",
    srcs = ["split_swagger_json.py"],
    visibility = ["//visibility:public"],
)
