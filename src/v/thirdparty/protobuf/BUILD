#
# TODO(bazel) this is a temporary hack to fix layering check violation with
# protobuf v24 until we upgrade to v26, which doesn't seem to have the problem,
# but can't immediately move to v26 without dealing with some depcreated
# interfaces. The `.h` file is not listed anywhere as a public header, and the
# `.pb.h` beats me. all of the other proto files work as expected.
#
cc_library(
    name = "descriptor",
    hdrs = [
        "descriptor.h",
        "descriptor.pb.h",
    ],
    include_prefix = "thirdparty/protobuf",
    visibility = ["//visibility:public"],
    deps = [
        "@protobuf",
    ],
)
