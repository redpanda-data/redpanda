load("@bazel_skylib//lib:selects.bzl", "selects")
load("@bazel_skylib//rules:common_settings.bzl", "bool_flag", "string_flag")

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

# Selects the sanitizer configuration to use for this build.
# Note that simply setting this flag is not enough, you also need
# to enable specific additional flags (see .bazelrc) to actually
# enable the sanitizers. This flag is set *in addition* to those
# flags so that downstream build files can made local decisions
# based on whether sanitizers are enabled or not (e.g., krb5).
string_flag(
    name = "sanitizer_mode",
    build_setting_default = "none",
    values = [
        "none",
        "asan",
        "all",
    ],
)

config_setting(
    name = "sanitizers_none",
    flag_values = {
        ":sanitizer_mode": "none",
    },
)

config_setting(
    name = "sanitizers_asan",
    flag_values = {
        ":sanitizer_mode": "asan",
    },
)

config_setting(
    name = "sanitizers_all",
    flag_values = {
        ":sanitizer_mode": "all",
    },
)

selects.config_setting_group(
    name = "enable_fuzz_testing",
    match_any = [
        ":sanitizers_asan",
        ":sanitizers_all",
    ],
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

bool_flag(
    name = "io_uring",
    build_setting_default = False,
)

config_setting(
    name = "io_uring_enabled",
    flag_values = {
        ":io_uring": "true",
    },
)

bool_flag(
    name = "antithesis",
    build_setting_default = False,
)

config_setting(
    name = "antithesis_enabled",
    flag_values = {
        ":antithesis": "true",
    },
)
