load("@bazel_skylib//rules:select_file.bzl", "select_file")
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

filegroup(
    name = "gen_dir",
    srcs = [":openssl-fips"],
    output_group = "gen_dir",
    visibility = [
        "//visibility:public",
    ],
)

select_file(
    name = "fipsmodule_so",
    srcs = ":openssl-fips",
    subpath = "lib/ossl-modules/fips.so",
    visibility = [
        "//visibility:public",
    ],
)
