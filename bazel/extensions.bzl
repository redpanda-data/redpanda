"""
This module contains extensions for working with Bazel modules.
"""

load("//bazel:repositories.bzl", "data_dependency")

def _non_module_dependencies_impl(_ctx):
    data_dependency()

non_module_dependencies = module_extension(
    implementation = _non_module_dependencies_impl,
)
