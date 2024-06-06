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
