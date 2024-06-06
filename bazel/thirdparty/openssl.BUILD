load("@rules_foreign_cc//foreign_cc:defs.bzl", "configure_make")

# TODO
# - handle --debug vs --release mode flags
# - package up the fips.so module file

filegroup(
    name = "srcs",
    srcs = glob(["**"]),
)

configure_make(
    name = "openssl",
    configure_command = "Configure",
    configure_options = [
        "enable-fips",
        "--debug",
        "--libdir=lib",
    ],
    lib_source = ":srcs",
    out_shared_libs = [
        "libssl.so",
        "libcrypto.so",
    ],
    visibility = [
        "//visibility:public",
    ],
)
