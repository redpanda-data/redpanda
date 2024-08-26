"""
This module contains functions for working with Avro.
"""

load("//bazel:build.bzl", "redpanda_cc_library")

def avro_cc_library(name, class_name, schema, namespace, include_prefix = None, visibility = None):
    """
    Generate Avro library.

    Args:
      name: name of the library
      class_name: name of the generated class
      schema: the schema file
      namespace: namespace for generated code
      include_prefix: header include prefix
      visibility: library visibility
    """
    hh_out = class_name + ".avrogen.h"

    native.genrule(
        name = class_name + "_genrule",
        srcs = [schema],
        outs = [hh_out],
        cmd = "$(location @avro//:avrogen) -i $< -o $@ -n " + namespace,
        tools = ["@avro//:avrogen"],
    )

    redpanda_cc_library(
        name = name,
        hdrs = [
            hh_out,
        ],
        visibility = visibility,
        include_prefix = include_prefix,
        deps = [
            "@avro",
        ],
    )
