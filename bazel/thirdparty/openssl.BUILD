load("@bazel_skylib//rules:common_settings.bzl", "int_flag", "string_flag")
load("@bazel_skylib//rules:select_file.bzl", "select_file")
load("@rules_cc//cc:cc_library.bzl", "cc_library")
load("@rules_foreign_cc//foreign_cc:defs.bzl", "configure_make", "runnable_binary")

# Make this build faster by setting `build --@openssl//:build_jobs=16` in user.bazelrc
# if you have the cores to spare.
int_flag(
    name = "build_jobs",
    build_setting_default = 8,
    make_variable = "BUILD_JOBS",
    visibility = ["@openssl-fips//:__pkg__"],
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
    name = "openssl_foreign_cc",
    # These don't get make variables expanded, so use the injected environment variable.
    args = [
        "-j$OPENSSL_BUILD_JOBS",
        # This will cause OpenSSL to ignore the prefix flag and install at the specified DESTDIR
        "DESTDIR=$BUILD_TMPDIR/openssl_foreign_cc",
    ],
    configure_command = "Configure",
    configure_options = [
        # OpenSSL will look for system certs in a path relative to the OPENSSLDIR macro
        # This macro is defined as <prefix>/<openssldir>
        # Most linux environments install system certs in /etc/ssl/certs, so we set
        # the OPENSSLDIR macro to /etc/ssl
        "--prefix=/",
        "--openssldir=/etc/ssl",
        "--libdir=lib",
        "no-tests",
        "no-docs",
    ] + select({
        ":debug_mode": ["--debug"],
        ":release_mode": ["--release"],
        "//conditions:default": [],
    }),
    env = {
        "OPENSSL_BUILD_JOBS": "$(BUILD_JOBS)",
    },
    lib_source = ":srcs",
    out_binaries = [
        "openssl",
    ],
    out_data_dirs = [
        "etc/ssl",
    ],
    out_shared_libs = [
        "libssl.so.3",
        "libcrypto.so.3",
    ],
    toolchains = [":build_jobs"],
    visibility = [
        "//visibility:public",
    ],
)

# This is only the plain binary and does not have LD_LIBRARY_PATH
# properly setup, so don't expect to be able to easily run this
# binary and it pickup the proper shared libraries.
filegroup(
    name = "openssl_binary",
    srcs = [":openssl_foreign_cc"],
    output_group = "openssl",
    visibility = [
        "//visibility:public",
    ],
)

select_file(
    name = "openssl_data",
    srcs = ":openssl_foreign_cc",
    subpath = "ssl",
    visibility = [
        "//visibility:public",
    ],
)

# If you need to bazel_run or use a genrule with openssl, this is the target you want
runnable_binary(
    name = "openssl_exe",
    binary = "openssl",
    foreign_cc_target = ":openssl_foreign_cc",
    visibility = ["//visibility:public"],
)

cc_library(
    name = "openssl",
    hdrs = [
        ":openssl_foreign_cc",
    ],
    includes = ["openssl_foreign_cc/include"],
    visibility = ["//visibility:public"],
    deps = [
        ":openssl_foreign_cc",
    ],
)
