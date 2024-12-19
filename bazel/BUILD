load("@bazel_skylib//rules:common_settings.bzl", "bool_flag")

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
