#
# This build is a translation of the CRoaring cmake-based build.
#
# TODO(bazel)
# - Take a closer look at compile options that control cpu features.
#

cc_library(
    name = "roaring",
    srcs = glob([
        "src/**/*.c",
    ]),
    hdrs = glob([
        "include/roaring/**/*.h",
        "include/roaring/**/*.hh",
    ]),
    copts = [
        "-DDISABLE_NEON=1",
        "-DROARING_DISABLE_AVX=1",
        "-Wno-unused-function",
    ],
    strip_include_prefix = "include",
    visibility = [
        "//visibility:public",
    ],
)
