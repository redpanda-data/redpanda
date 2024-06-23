load("//bazel:build.bzl", "redpanda_cc_library")

def redpanda_cc_rpc_library(name, src, out = None, visibility = None):
    if not src.endswith(".json"):
        fail(src, "expected to have .json suffix")

    # the convention is to generate x_service.h from x.json
    if not out:
        out = src.removesuffix(".json") + "_service.h"

    native.genrule(
        name = name + "_genrule",
        srcs = [src],
        outs = [out],
        cmd = "$(location //src/v/rpc:compiler) --service_file $< --output_file $@",
        tools = ["//src/v/rpc:compiler"],
    )

    redpanda_cc_library(
        name = name,
        hdrs = [out],
        visibility = visibility,
    )
