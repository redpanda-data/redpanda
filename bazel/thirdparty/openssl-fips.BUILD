load("@rules_foreign_cc//foreign_cc:defs.bzl", "configure_make")

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
        "@openssl//:debug_mode": ["--debug"],
        "@openssl//:release_mode": ["--release"],
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
