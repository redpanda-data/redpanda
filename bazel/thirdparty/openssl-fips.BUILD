load("@bazel_skylib//rules:common_settings.bzl", "string_flag")
load("@rules_foreign_cc//foreign_cc:defs.bzl", "configure_make")

string_flag(
    name = "build_mode",
    build_setting_default = "default",
    values = [
        "debug",
        "release",
        "default",
    ],
)

config_setting(
    name = "debug_mode",
    flag_values = {
        ":build_mode": "debug",
    },
)

config_setting(
    name = "release_mode",
    flag_values = {
        ":build_mode": "release",
    },
)

filegroup(
    name = "srcs",
    srcs = glob(["**"]),
)

configure_make(
    name = "openssl-fips",
    configure_command = "Configure",
    configure_options = [
        "enable-fips",
        "--libdir=lib",
    ] + select({
        ":debug_mode": ["--debug"],
        ":release_mode": ["--release"],
        "//conditions:default": [],
    }),
    lib_source = ":srcs",
    out_shared_libs = [
        "ossl-modules/fips.so",
    ],
    targets = [
        "",
        "install_fips",
    ],
    visibility = [
        "//visibility:public",
    ],
)
