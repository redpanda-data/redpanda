# Heavily inspired by protobuf rules in rules_rust

"""
Redpanda specific protobuf rules.
"""

load("@bazel_skylib//lib:paths.bzl", "paths")
load("@rules_proto//proto:defs.bzl", "ProtoInfo")
load("@rules_proto//proto:proto_common.bzl", proto_toolchains = "toolchains")
load("//bazel:build.bzl", "redpanda_cc_library")

RedpandaProtoInfo = provider(
    doc = "Redpanda protobuf provider info",
    fields = {
        "proto_sources": "List[string]: list of source paths of protos",
        "transitive_proto_sources": "depset[string]",
    },
)

def _compute_proto_source_path(file, source_root):
    """Take the short path of file and make it suitable for protoc.

    Args:
        file (File): The target source file.
        source_root (str): The directory relative to which the `.proto` \
            files defined in the proto_library are defined.

    Returns:
        str: The protoc suitible path of `file`
    """

    # Bazel creates symlinks to the .proto files under a directory called
    # "_virtual_imports/<rule name>" if we do any sort of munging of import
    # paths (e.g. using strip_import_prefix / import_prefix attributes)
    virtual_imports = "/_virtual_imports/"
    if virtual_imports in file.path:
        return file.path.split(virtual_imports)[1].split("/", 1)[1]

    # For proto, they need to be requested with their absolute name to be
    # compatible with the descriptor_set passed by proto_library.
    # I.e. if you compile a protobuf at @repo1//package:file.proto, the proto
    # compiler would generate a file descriptor with the path
    # `package/file.proto`. Since we compile from the proto descriptor, we need
    # to pass the list of descriptors and the list of path to compile.
    # For the precedent example, the file (noted `f`) would have
    # `f.short_path` returns `external/repo1/package/file.proto`.
    # In addition, proto_library can provide a proto_source_path to change the base
    # path, which should a be a prefix.
    path = file.short_path

    # Strip external prefix.
    path = path.split("/", 2)[2] if path.startswith("../") else path

    # Strip source_root.
    if path.startswith(source_root):
        return path[len(source_root):]
    else:
        return path

def _redpanda_proto_aspect_impl(target, ctx):
    """The implementation of the `redpanda_proto_aspect` aspect

    Args:
        target (Target): The target to which the aspect is applied
        ctx (ctx): The rule context which the targetis created from

    Returns:
        list: A list containg a `RedpandaProtoInfo` provider
    """
    if ProtoInfo not in target:
        return None

    sources = [
        _compute_proto_source_path(f, source_root = "")
        for f in target[ProtoInfo].direct_sources
    ]
    transitive_sources = [
        f[RedpandaProtoInfo].transitive_proto_sources
        for f in ctx.rule.attr.deps
        if RedpandaProtoInfo in f
    ]
    return [RedpandaProtoInfo(
        proto_sources = sources,
        transitive_proto_sources = depset(transitive = transitive_sources, direct = sources),
    )]

_redpanda_proto_aspect = aspect(
    doc = "An aspect that gathers Redpanda proto direct and transitive sources",
    implementation = _redpanda_proto_aspect_impl,
    attr_aspects = ["deps"],
)

def _redpanda_protogen_impl(ctx):
    """Implementation of the redpanda_proto_library.

    Args:
        ctx (ctx): The current rule's context object

    Returns:
        list: A list of providers
    """
    proto = [dep[ProtoInfo] for dep in ctx.attr.deps if ProtoInfo in dep]
    direct_sources = []
    transitive_sources = []
    for proto_info in [f[RedpandaProtoInfo] for f in ctx.attr.deps if RedpandaProtoInfo in f]:
        direct_sources.extend(proto_info.proto_sources)
        transitive_sources.append(proto_info.transitive_proto_sources)

    protoc_plugin = ctx.file._protoc_plugin
    proto_toolchain = proto_toolchains.find_toolchain(
        ctx,
        legacy_attr = None,
        toolchain_type = "@rules_proto//proto:toolchain_type",
    )
    protoc = proto_toolchain.proto_compiler

    args = ctx.actions.args()
    imports = depset(transitive = [p.transitive_imports for p in proto])
    if not direct_sources:
        fail("Protobuf compilation requested without inputs!")
    proto_srcs = [src for p in proto for src in p.direct_sources]
    cc_hdrs = []
    cc_srcs = []
    for src in proto_srcs:
        header_name = paths.basename(paths.replace_extension(src.path, ".proto.h"))
        impl_name = paths.basename(paths.replace_extension(src.path, ".proto.cc"))
        cc_hdrs.append(ctx.actions.declare_file(header_name))
        cc_srcs.append(ctx.actions.declare_file(impl_name))
    args.add_all([
        "--plugin=protoc-gen-redpanda=" + protoc_plugin.path,
        "--redpanda_out=" + cc_srcs[0].dirname,
    ])
    descriptor_sets = depset(transitive = [p.transitive_descriptor_sets for p in proto])
    args.add_joined(
        descriptor_sets,
        join_with = ":",
        format_joined = "--descriptor_set_in=%s",
    )
    args.add_all(direct_sources)
    ctx.actions.run(
        inputs = depset(transitive = [descriptor_sets, imports]),
        tools = [protoc_plugin, protoc.executable],
        outputs = cc_hdrs + cc_srcs,
        executable = protoc.executable,
        arguments = [args],
        progress_message = "Generating Redpanda protobuf stubs",
        mnemonic = "RedpandaProtoGen",
    )
    return [
        DefaultInfo(files = depset(cc_hdrs + cc_srcs)),
        OutputGroupInfo(
            cc_hdrs = depset(cc_hdrs),
            cc_srcs = depset(cc_srcs),
        ),
    ]

_redpanda_protogen = rule(
    implementation = _redpanda_protogen_impl,
    attrs = {
        "deps": attr.label_list(
            mandatory = True,
            providers = [ProtoInfo],
            aspects = [_redpanda_proto_aspect],
        ),
        "_protoc_plugin": attr.label(
            allow_single_file = True,
            default = "//bazel/pbgen",
        ),
    },
    toolchains = [
        "@rules_proto//proto:toolchain_type",
    ],
)

def redpanda_proto_library(name, protos, deps = [], **kwargs):
    """
    Creates a Redpanda specific proto_library.

    Args:
        name (str): The name of the target
        protos (list): A list of proto_library dependencies that will be built.
        deps (list): Other `redpanda_proto_library` targets that contains C++
                     that these generated protos rely on (this will likely mirror
                     dependencies of the original proto_library).
        **kwargs: Additional keyword arguments to pass to the rule
    Returns:
        None

    Example:

    ```python
    load("//bazel/pbgen/pbgen.bzl", "redpanda_proto_library")

    proto_library(
        name = "my_proto",
        srcs = ["my.proto"]
    )

    redpanda_proto_library(
        name = "my_redpanda_proto",
        protos = [":my_proto"],
    )

    proto_library(
        name = "other_proto",
        srcs = ["other.proto"]
        deps = [":my_proto"]
    )

    redpanda_proto_library(
        name = "other_redpanda_proto",
        protos = [":other_proto"],
        deps = [":my_redpanda_proto"],
    )

    redpanda_cc_binary(
        name = "my_proto_binary",
        srcs = ["main.cc"],
        deps = [":other_redpanda_proto"],
    )
    ```
    """
    _redpanda_protogen(
        name = name + "_generated_files",
        deps = protos,
        visibility = ["//visibility:private"],
    )
    native.filegroup(
        name = name + "_generated_hdrs",
        srcs = [name + "_generated_files"],
        output_group = "cc_hdrs",
        visibility = ["//visibility:private"],
    )
    native.filegroup(
        name = name + "_generated_srcs",
        srcs = [name + "_generated_files"],
        output_group = "cc_srcs",
        visibility = ["//visibility:private"],
    )
    redpanda_cc_library(
        name = name,
        srcs = [name + "_generated_srcs"],
        hdrs = [name + "_generated_hdrs"],
        implementation_deps = [
            "//src/v/serde/protobuf:wire_format",
            "//src/v/serde/protobuf:support",
            "//src/v/serde/protobuf:json",
            "//src/v/serde/json:writer",
            "//src/v/serde/json:parser",
            "//src/v/bytes:iostream",
            "//src/v/utils:to_string",
            "//src/v/bytes:iobuf_parser",
            "@fmt",
        ],
        deps = [
            "//src/v/base",
            "//src/v/bytes:iobuf",
            "//src/v/serde/protobuf:rpc",
            "//src/v/strings:static_str",
            "//src/v/serde/protobuf:base",
            "//src/v/container:chunked_hash_map",
            "//src/v/container:chunked_vector",
            "@seastar",
        ] + deps,
        **kwargs
    )
