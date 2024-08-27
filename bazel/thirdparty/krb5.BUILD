load("@rules_foreign_cc//foreign_cc:defs.bzl", "configure_make")

filegroup(
    name = "srcs",
    srcs = glob(["**"]),
)

configure_make(
    name = "krb5",
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
    lib_source = ":srcs",
    out_shared_libs = [
        "libgssapi_krb5.so.2",
        "libk5crypto.so.3",
        "libkrb5.so.3",
        "libkrb5support.so.0",
    ],
    deps = [
        "@openssl",
    ],
    visibility = [
        "//visibility:public",
    ],
)
