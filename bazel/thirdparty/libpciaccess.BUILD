#
# This build is a translation of the libpciaccess meson build:
# https://gitlab.freedesktop.org/xorg/lib/libpciaccess/-/blob/2ec2576cabefef1eaa5dd9307c97de2e887fc347/meson.build
#
cc_library(
    name = "libpciaccess",
    srcs = [
        "src/common_bridge.c",
        "src/common_capability.c",
        "src/common_device_name.c",
        "src/common_init.c",
        "src/common_interface.c",
        "src/common_io.c",
        "src/common_iterator.c",
        "src/common_map.c",
        "src/linux_devmem.h",
        "src/pciaccess_private.h",
    ] + select({
        "@platforms//os:linux": [
            "src/common_vgaarb.c",
            "src/linux_devmem.c",
            "src/linux_sysfs.c",
        ],
        "//conditions:default": [],
    }),
    hdrs = [
        "include/pciaccess.h",
    ],
    copts = [
        "-Wpointer-arith",
        "-Wmissing-declarations",
        "-Wformat=2",
        "-Wstrict-prototypes",
        "-Wmissing-prototypes",
        "-Wnested-externs",
        "-Wbad-function-cast",
        "-Wold-style-definition",
        "-Wdeclaration-after-statement",
        "-Wunused",
        "-Wuninitialized",
        "-Wshadow",
        "-Wmissing-noreturn",
        "-Wmissing-format-attribute",
        "-Wredundant-decls",
        "-Werror=implicit",
        "-Werror=nonnull",
        "-Werror=init-self",
        "-Werror=main",
        "-Werror=missing-braces",
        "-Werror=sequence-point",
        "-Werror=return-type",
        "-Werror=trigraphs",
        "-Werror=array-bounds",
        "-Werror=write-strings",
        "-Werror=address",
        "-Werror=int-to-pointer-cast",

        # Quiet some build warnings
        "-Wno-unused-result",
        "-Wno-tautological-constant-out-of-range-compare",
    ],
    # This is only consumed by a make library that produces a static library,
    # so this library only needs to produce a static library. Doing this means
    # that the final build doesn't have a worthless shared library copied to
    # the output.
    linkstatic = True,
    local_defines = [
        'PCIIDS_PATH=\\"/usr/share/hwdata\\"',
        "HAVE_ZLIB=1",
        "HAVE_MTRR=1",
        "HAVE_ERR_H=1",
        "HAVE_INTTYPES_H=1",
        "HAVE_STDINT_H=1",
        "HAVE_STRINGS_H=1",
        "HAVE_STRING_H=1",
    ],
    strip_include_prefix = "include",
    visibility = [
        "//visibility:public",
    ],
    deps = [
        "@zlib",
    ],
)
