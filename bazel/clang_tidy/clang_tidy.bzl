# MIT License
#
# Copyright (c) 2020 Benedek Thaler
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.
#
# This file was forked from https://github.com/erenon/bazel_clang_tidy, with fixes
# and functionally added specifically for Redpanda.

"""
A aspect to run clang-tidy on C/C++ source files.

See: https://bazel.build/extending/aspects
See: https://clang.llvm.org/extra/clang-tidy/
"""

load("@bazel_tools//tools/build_defs/cc:action_names.bzl", "ACTION_NAMES")
load("@bazel_tools//tools/cpp:toolchain_utils.bzl", "find_cpp_toolchain")
load("@rules_cc//cc/common:cc_common.bzl", "cc_common")
load("@rules_cc//cc/common:cc_info.bzl", "CcInfo")

def _run_tidy(
        ctx,
        wrapper,
        exe,
        config,
        plugins,
        flags,
        infile,
        discriminator,
        additional_files,
        additional_inputs):
    py_toolchain = ctx.toolchains[Label("@rules_python//python:toolchain_type")]
    cc_toolchain = find_cpp_toolchain(ctx)
    direct_inputs = (
        [infile, config, plugins] +
        ([exe.files_to_run.executable] if exe.files_to_run.executable else [])
    )
    for additional_input in additional_inputs:
        direct_inputs.extend(additional_input.files.to_list())

    inputs = depset(
        direct = direct_inputs,
        transitive = [additional_files, cc_toolchain.all_files],
    )

    outfile = ctx.actions.declare_file(
        "bazel_clang_tidy_" + infile.path + "." + discriminator + ".clang-tidy.yaml",
    )

    args = ctx.actions.args()

    args.add(exe.files_to_run.executable)
    args.add(outfile.path)
    args.add(config.path)
    args.add(plugins.path)

    # add source to check
    args.add(infile.path)

    # start args passed to the compiler
    args.add("--")

    ctx.actions.run(
        inputs = inputs,
        outputs = [outfile],
        tools = [py_toolchain.py3_runtime.interpreter],
        executable = wrapper,
        arguments = [args] + flags,
        mnemonic = "ClangTidy",
        use_default_shell_env = True,
        progress_message = "Run clang-tidy on {}".format(infile.short_path),
    )
    return outfile

def rule_sources(attr, include_headers):
    """
    Returns a list of source files from the given rule attributes.

    Args:
      attr: The rule attributes to check for source files.
      include_headers: If True, include header files in the returned list.

    Returns:
      A list of source files (File objects) that match the permitted file types.
      If include_headers is False, header files are excluded.
    """
    header_extensions = (
        ".h",
        ".hh",
        ".hpp",
        ".hxx",
        ".inc",
        ".inl",
        ".H",
    )
    permitted_file_types = [
        ".c",
        ".cc",
        ".cpp",
        ".cxx",
        ".c++",
        ".C",
    ] + list(header_extensions)

    def check_valid_file_type(src):
        """
        Returns True if the file type matches one of the permitted srcs file types for C and C++ header/source files.
        """
        for file_type in permitted_file_types:
            if src.basename.endswith(file_type):
                return True
        return False

    srcs = []
    if hasattr(attr, "srcs"):
        for src in attr.srcs:
            srcs += [s for s in src.files.to_list() if s.is_source and check_valid_file_type(s)]
    if hasattr(attr, "hdrs"):
        for hdr in attr.hdrs:
            srcs += [h for h in hdr.files.to_list() if h.is_source and check_valid_file_type(h)]
    if include_headers:
        return srcs
    else:
        return [src for src in srcs if not src.basename.endswith(header_extensions)]

def toolchain_flags(ctx, action_name = ACTION_NAMES.cpp_compile):
    """
    Returns the flags for the given action name based on the C/C++ toolchain.

    Args:
      ctx: The context of the rule or aspect.
      action_name: The action name for which to get the flags (default is ACTION_NAMES.cpp_compile).

    Returns:
      A list of flags for the specified action name.
    """
    cc_toolchain = find_cpp_toolchain(ctx)
    feature_configuration = cc_common.configure_features(
        ctx = ctx,
        cc_toolchain = cc_toolchain,
    )
    user_compile_flags = ctx.fragments.cpp.copts
    if action_name == ACTION_NAMES.cpp_compile:
        user_compile_flags.extend(ctx.fragments.cpp.cxxopts)
    elif action_name == ACTION_NAMES.c_compile and hasattr(ctx.fragments.cpp, "conlyopts"):
        user_compile_flags.extend(ctx.fragments.cpp.conlyopts)
    compile_variables = cc_common.create_compile_variables(
        feature_configuration = feature_configuration,
        cc_toolchain = cc_toolchain,
        user_compile_flags = user_compile_flags,
    )
    flags = cc_common.get_memory_inefficient_command_line(
        feature_configuration = feature_configuration,
        action_name = action_name,
        variables = compile_variables,
    )
    return flags

def deps_flags(deps):
    """
    Returns the flags and additional files for the given dependencies.

    Args:
      deps: A list of dependencies (targets) to extract flags and additional files from.

    Returns:
      A tuple containing:
      - A list of flags extracted from the dependencies.
      - A depset of additional files (headers) from the dependencies.
    """
    compilation_contexts = [dep[CcInfo].compilation_context for dep in deps]
    additional_files = depset(transitive = [
        compilation_context.headers
        for compilation_context in compilation_contexts
    ])

    flags = []

    for compilation_context in compilation_contexts:
        # add defines
        for define in compilation_context.defines.to_list():
            flags.append("-D" + define)

        for define in compilation_context.local_defines.to_list():
            flags.append("-D" + define)

        # add includes
        for i in compilation_context.framework_includes.to_list():
            flags.append("-F" + i)

        for i in compilation_context.includes.to_list():
            flags.append("-I" + i)

        for i in compilation_context.quote_includes.to_list():
            flags.extend(["-iquote", i])

        for i in compilation_context.system_includes.to_list():
            flags.extend(["-isystem", i])

        for i in compilation_context.external_includes.to_list():
            flags.extend(["-isystem", i])

    return flags, additional_files

def is_c_translation_unit(src, tags):
    """Judge if a source file is for C.

    Args:
        src(File): Source file object.
        tags(list[str]): Tags attached to the target.

    Returns:
        bool: Whether the source is for C.
    """
    if "clang-tidy-is-c-tu" in tags:
        return True

    return src.extension == "c"

def _clang_tidy_aspect_impl(target, ctx):
    # Ignore external targets
    if target.label.workspace_root.startswith("external"):
        return []

    # Targets with specific tags will not be formatted
    ignore_tags = [
        "noclangtidy",
        "no-clang-tidy",
    ]

    for tag in ignore_tags:
        if tag in ctx.rule.attr.tags:
            return []

    wrapper = ctx.attr._clang_tidy_wrapper.files_to_run
    exe = ctx.attr._clang_tidy_executable
    config = ctx.attr._clang_tidy_config.files.to_list()[0]
    plugins = ctx.attr._clang_tidy_plugins.files.to_list()[0]

    deps = [target] + getattr(ctx.rule.attr, "implementation_deps", [])
    rule_flags, additional_files = deps_flags(deps)
    copts = ctx.rule.attr.copts if hasattr(ctx.rule.attr, "copts") else []
    for copt in copts:
        rule_flags.append(ctx.expand_make_variables(
            "copts",
            copt,
            {},
        ))

    c_flags = toolchain_flags(ctx, ACTION_NAMES.c_compile) + rule_flags + ["-xc"]
    cxx_flags = toolchain_flags(ctx, ACTION_NAMES.cpp_compile) + rule_flags + ["-xc++"]

    srcs = rule_sources(ctx.rule.attr, False)

    outputs = [
        _run_tidy(
            ctx,
            wrapper,
            exe,
            config,
            plugins,
            c_flags if is_c_translation_unit(src, ctx.rule.attr.tags) else cxx_flags,
            src,
            target.label.name,
            additional_files,
            additional_inputs = getattr(ctx.rule.attr, "additional_compiler_inputs", []),
        )
        for src in srcs
    ]

    return [
        OutputGroupInfo(report = depset(direct = outputs)),
    ]

clang_tidy_aspect = aspect(
    implementation = _clang_tidy_aspect_impl,
    fragments = ["cpp"],
    attr_aspects = ["implementation_deps"],
    attrs = {
        "_cc_toolchain": attr.label(default = Label("@bazel_tools//tools/cpp:current_cc_toolchain")),
        "_clang_tidy_wrapper": attr.label(default = Label("//bazel/clang_tidy")),
        "_clang_tidy_executable": attr.label(default = Label("@current_llvm_toolchain//:clang-tidy")),
        "_clang_tidy_config": attr.label(default = Label("//:clang_tidy_config")),
        "_clang_tidy_plugins": attr.label(default = Label("//bazel/clang_tidy/plugins:plugins.so")),
    },
    required_providers = [[CcInfo]],
    toolchains = [
        "@bazel_tools//tools/cpp:toolchain_type",
        "@rules_python//python:toolchain_type",
    ],
)
