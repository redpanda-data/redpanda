load("@rules_foreign_cc//foreign_cc:defs.bzl", "configure_make")

filegroup(
    name = "srcs",
    srcs = glob(["**"]),
)

configure_make(
    name = "hwloc",
    autoreconf = True,
    autoreconf_options = ["-ivf"],
    configure_in_place = True,
    configure_options = [
        "--disable-libudev",
        "--disable-shared",
        "--enable-static",
    ],
    lib_source = ":srcs",
    out_static_libs = ["libhwloc.a"],
    visibility = [
        "//visibility:public",
    ],
    deps = [
        "@libpciaccess",
    ],
)
