"""
Bazel rules for working with Swagger/OpenAPI specifications.
"""

def split_swagger_json(
        name,
        src,
        output_basename = None,
        visibility = None):
    """
    Split a full Swagger JSON file into component files for Seastar tooling.

    This macro generates three files from a full Swagger 2.0 JSON specification:
    1. {output_basename}_header.json - Contains swagger metadata (version, info, host, etc.)
    2. {output_basename}_definitions.def.json - Contains the definitions section
    3. {output_basename}.json - Contains the paths section

    These split files are required by Seastar's json2code tool for C++ code generation.
    By maintaining the full specification and auto-generating the split files, you can:
    - Use the full spec for testing, documentation, and client generation
    - Automatically keep the split files in sync with the full spec
    - Avoid manual maintenance of multiple files

    Example usage:
    ```python
    load("//bazel:swagger.bzl", "split_swagger_json")
    load("//bazel/thirdparty:seastar.bzl", "seastar_cc_swagger_library")

    split_swagger_json(
        name = "split_api_json",
        src = "api-doc/api_full.json",
        output_basename = "api",
    )

    seastar_cc_swagger_library(
        name = "api_swagger",
        src = "api.json",  # References the generated file
    )
    ```

    Args:
      name: Name of the genrule
      src: The full Swagger JSON file to split
      output_basename: Base name for output files. If not provided, uses the name parameter
      visibility: Visibility of the generated files
    """
    if output_basename == None:
        output_basename = name

    header_out = output_basename + "_header.json"
    definitions_out = output_basename + "_definitions.def.json"
    paths_out = output_basename + ".json"

    native.genrule(
        name = name,
        srcs = [src],
        outs = [
            header_out,
            definitions_out,
            paths_out,
        ],
        cmd = "$(location //bazel:split_swagger_json) $(location " + src + ") $(@D) " + output_basename,
        tools = ["//bazel:split_swagger_json"],
        visibility = visibility,
    )
