# See google/libprotobuf-mutator#91

cc_library(
    name = "libprotobuf_mutator",
    testonly = 1,
    srcs = glob(
        [
            "src/*.cc",
            "src/*.h",
        ],
        exclude = [
            "**/*_test.cc",
            "src/mutator.h",
        ],
    ) + [
        "port/protobuf.h",
    ],
    hdrs = [
        "src/mutator.h",
    ],
    include_prefix = "protobuf_mutator",
    strip_include_prefix = "src",
    visibility = ["//visibility:public"],
    deps = ["@protobuf"],
)
