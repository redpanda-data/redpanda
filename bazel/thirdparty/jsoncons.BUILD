#
# This build is a translation of the header-only library cmake-based build in
# the jsoncons source tree.
#

cc_library(
    name = "jsoncons",
    hdrs = glob([
        "include/jsoncons/**/*.hpp",
        "include/jsoncons_ext/**/*.hpp",
    ]),
    strip_include_prefix = "include",
    visibility = [
        "//visibility:public",
    ],
)
