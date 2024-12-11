load("@bazel_skylib//rules:common_settings.bzl", "bool_flag")

bool_flag(
    name = "mark_optimized",
    build_setting_default = False,
)

config_setting(
    name = "optimized_build",
    flag_values = {
        ":mark_optimized": "true",
    },
)
