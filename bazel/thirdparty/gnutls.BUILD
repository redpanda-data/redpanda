load("@rules_foreign_cc//foreign_cc:defs.bzl", "configure_make")

filegroup(
    name = "srcs",
    srcs = glob(["**"]),
)

configure_make(
    name = "gnutls",
    autoreconf = True,
    autoreconf_options = ["-ivf"],
    configure_in_place = True,
    configure_options = [
        "--with-included-unistring",
        "--with-included-libtasn1",
        "--without-idn",
        "--without-brotli",
        "--without-p11-kit",
        # https://lists.gnupg.org/pipermail/gnutls-help/2016-February/004085.html
        "--disable-non-suiteb-curves",
        "--disable-doc",
        "--disable-tests",
        "--disable-shared",
        "--enable-static",
    ],
    env = {"GTKDOCIZE": "echo"},
    lib_source = ":srcs",
    out_static_libs = ["libgnutls.a"],
    visibility = [
        "//visibility:public",
    ],
    deps = [
        "@nettle",
    ],
)
