cc_library(
    name = "avro",
    srcs = glob([
        "lang/c++/impl/**/*.cc",
        "lang/c++/impl/**/*.hh",
    ]),
    hdrs = glob([
        "lang/c++/include/avro/**/*.hh",
    ]),
    copts = [
        "-Wno-unused-but-set-variable",
    ],
    defines = ["SNAPPY_CODEC_AVAILABLE"],
    includes = [
        "lang/c++/include",
        # TODO this one is here because the source files assume they can include
        # without the avro prefix. it'd be nice to avoid leaking this path out
        # to anything that depends on avro. according to the docs, it sounds
        # like adding -I to copts for the private compile options is the right
        # way to go, but so far I haven't been able to get the path correct.
        "lang/c++/include/avro",
    ],
    local_defines = ["AVRO_VERSION=1"],
    visibility = [
        "//visibility:public",
    ],
    deps = [
        "@boost",
    ],
)
