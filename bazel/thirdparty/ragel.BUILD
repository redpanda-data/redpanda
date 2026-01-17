load("@bazel_skylib//rules:common_settings.bzl", "int_flag")
load("@rules_foreign_cc//foreign_cc:defs.bzl", "configure_make")

# Make this build faster by setting `build --@ragel//:build_jobs=16` in user.bazelrc
# if you have the cores to spare.
int_flag(
    name = "build_jobs",
    build_setting_default = 8,
    make_variable = "BUILD_JOBS",
)

filegroup(
    name = "srcs",
    srcs = glob(["**"]),
)

configure_make(
    name = "ragel",
    args = ["-j$RAGEL_BUILD_JOBS"],
    autoreconf = True,
    autoreconf_options = ["-ivf"],
    configure_in_place = True,
    configure_options = [
        # Build a static binary for better portability
        "--disable-shared",
        "--disable-manual",
    ],
    env = {
        "RAGEL_BUILD_JOBS": "$(BUILD_JOBS)",
    },
    lib_source = ":srcs",
    out_binaries = ["ragel"],
    targets = ["install-exec"],
    toolchains = [":build_jobs"],
    visibility = [
        "//visibility:public",
    ],
)

filegroup(
    name = "ragel_bin",
    srcs = [":ragel"],
    output_group = "ragel",
    visibility = ["//visibility:public"],
)
