load("@bazel_skylib//rules:common_settings.bzl", "int_flag")
load("@rules_foreign_cc//foreign_cc:defs.bzl", "configure_make")

# Make this build faster by setting `build --@krb5//:build_jobs=8` in user.bazelrc
# if you have the cores to spare
int_flag(
    name = "build_jobs",
    build_setting_default = 4,
    make_variable = "BUILD_JOBS",
)

filegroup(
    name = "srcs",
    srcs = glob(["**"]),
)

configure_make(
    name = "krb5",
    # These don't get make variables expanded, so use the injected environment variable.
    args = ["-j$KRB5_BUILD_JOBS"],
    autoreconf = True,
    autoreconf_options = ["-ivf ./src"],
    configure_command = "./src/configure",
    configure_in_place = True,
    configure_options = [
        "--srcdir=./src",
        "--disable-thread-support",
        "--with-crypto-impl=openssl",
        "--with-tls-impl=openssl",
        # Normally, these libraries are auto detected,
        # but we never want them so explicitly disable
        # them.
        "--without-netlib",
        "--without-keyutils",
        "--without-lmdb",
        "--without-libedit",
        "--without-readline",
        "--without-system-verto",
        # TODO(bazel) when building the static library the linker is exiting with a
        # duplicate symbol error
        "--enable-shared",
        "--disable-static",
        # "--enable-asan=undefined,address",
    ],
    env = {
        "KRB5_BUILD_JOBS": "$(BUILD_JOBS)",
    },
    lib_source = ":srcs",
    out_shared_libs = [
        "libcom_err.so.3",
        "libgssapi_krb5.so.2",
        "libk5crypto.so.3",
        "libkrb5.so.3",
        "libkrb5support.so.0",
    ],
    toolchains = [":build_jobs"],
    visibility = [
        "//visibility:public",
    ],
    deps = [
        "@openssl",
    ],
)
