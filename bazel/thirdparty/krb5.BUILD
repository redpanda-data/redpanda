load("@bazel_skylib//rules:common_settings.bzl", "int_flag", "string_flag")
load("@rules_foreign_cc//foreign_cc:defs.bzl", "configure_make")

# Make this build faster by setting `build --@krb5//:build_jobs=8` in user.bazelrc
# if you have the cores to spare
int_flag(
    name = "build_jobs",
    build_setting_default = 4,
    make_variable = "BUILD_JOBS",
)

string_flag(
    name = "linker",
    build_setting_default = "lld",
    make_variable = "LINKER",
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
    configure_options = select({
        # When cross-compiling from macOS to Linux, configure tries to run
        # compiled test programs on the exec machine. Pass --host to tell it
        # the target architecture so it skips that check.
        "@platforms//cpu:x86_64": ["--host=x86_64-linux-gnu"],
        "@platforms//cpu:aarch64": ["--host=aarch64-linux-gnu"],
        "//conditions:default": [],
    }) + [
        # Pre-supply cache variables that require running test binaries,
        # which is not possible when cross-compiling from macOS to Linux.
        # __attribute__((constructor)) and __attribute__((destructor)) work
        # on modern Linux targets with clang.
        "krb5_cv_attr_constructor_destructor=yes,yes",
        "ac_cv_func_regcomp=yes",
        "ac_cv_printf_positional=yes",
        # AC_PROG_CC_STDC probes for C standards and appends the best one to CC.
        # With clang 20 it detects -std=gnu23, which removes K&R function
        # definitions used in util/ss/*.c. Setting ac_cv_prog_cc_c23 to the
        # empty string makes configure conclude "none needed" (C23 works without
        # an explicit flag), so it skips appending -std=gnu23 to CC. Clang 20
        # then defaults to gnu17, which still supports K&R.
        "ac_cv_prog_cc_c23=",
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
    ] + select({
        "@com_github_redpanda_data_redpanda//bazel:sanitizers_none": ["--enable-asan=no"],
        "@com_github_redpanda_data_redpanda//bazel:sanitizers_asan": ["--enable-asan=address"],
        "@com_github_redpanda_data_redpanda//bazel:sanitizers_all": ["--enable-asan=address,undefined,vptr,function,alignment"],
    }),
    copts = [
        "-fuse-ld=$LINKER",
    ],
    env = {
        # Need to pass this additionally here because of a bug in the kerberos build where it doesn't properly pass the linker flag down
        "KRB5_BUILD_JOBS": "$(BUILD_JOBS)",
        "LINKER": "$(LINKER)",
        # On Apple Silicon, Homebrew installs to /opt/homebrew/bin, which is not
        # in the default Bazel sandbox PATH (/bin:/usr/bin:/usr/local/bin).
        # autoreconf requires autoconf/automake from Homebrew, so add it here.
        "PATH": "/opt/homebrew/bin:/bin:/usr/bin:/usr/local/bin",
        # krb5's configure.ac checks ${WARN_CFLAGS+set}: if WARN_CFLAGS is set in
        # the environment (even as empty), it skips adding -Wall -Wmissing-prototypes
        # -Werror=... flags that would turn K&R C deprecation warnings into errors.
        # util/ss/*.c uses K&R C style which triggers -Wdeprecated-non-prototype.
        "WARN_CFLAGS": "",
    },
    lib_source = ":srcs",
    out_shared_libs = [
        "libcom_err.so.3",
        "libgssapi_krb5.so.2",
        "libk5crypto.so.3",
        "libkrb5.so.3",
        "libkrb5support.so.0",
    ],
    toolchains = [
        ":build_jobs",
        ":linker",
    ],
    visibility = [
        "//visibility:public",
    ],
    deps = [
        "@openssl//:openssl_foreign_cc",
    ],
)
