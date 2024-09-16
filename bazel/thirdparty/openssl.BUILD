load("@bazel_skylib//rules:common_settings.bzl", "int_flag", "string_flag")
load("@rules_foreign_cc//foreign_cc:defs.bzl", "configure_make")

# Make this build faster by setting `build --@openssl//:build_jobs=16` in user.bazelrc
# if you have the cores to spare.
int_flag(
    name = "build_jobs",
    build_setting_default = 8,
    make_variable = "BUILD_JOBS",
)

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
    name = "openssl",
    # These don't get make variables expanded, so use the injected environment variable.
    args = ["-j$OPENSSL_BUILD_JOBS"],
    configure_command = "Configure",
    configure_options = [
        "--libdir=lib",
    ] + select({
        ":debug_mode": ["--debug"],
        ":release_mode": ["--release"],
        "//conditions:default": [],
    }),
    env = {
        "OPENSSL_BUILD_JOBS": "$(BUILD_JOBS)",
    },
    lib_source = ":srcs",
    out_shared_libs = [
        "libssl.so.3",
        "libcrypto.so.3",
    ],
    toolchains = [":build_jobs"],
    visibility = [
        "//visibility:public",
    ],
)
