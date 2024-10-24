#
# This build is a translation of the header-only library cmake-based build in
# the rapidjson source tree.
#

cc_library(
    name = "rapidjson",
    hdrs = glob([
        "include/rapidjson/**/*.h",
    ]),
    defines = [
        "RAPIDJSON_HAS_STDSTRING",
    ],
    strip_include_prefix = "include",
    visibility = [
        "//visibility:public",
    ],
)
