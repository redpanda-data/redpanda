"""
A rule to create a redpanda tarball given inputs from the build system.
"""

load("@bazel_skylib//lib:collections.bzl", "collections")
load("@bazel_skylib//rules:common_settings.bzl", "BuildSettingInfo")
load("@bazel_tools//tools/cpp:toolchain_utils.bzl", "find_cpp_toolchain")

def _is_versioned(file, starts_with):
    """ Return true if this file has a name like libfoo.so.N """
    parts = file.basename.rsplit(".", 3)
    if len(parts) != 3:
        return False
    if not parts[0].startswith(starts_with):
        return False
    if parts[1] != "so":
        return False
    for c in parts[2].elems():
        if not c.isdigit():
            return False
    return True

def _is_versioned_so(file):
    return _is_versioned(file, "lib")

def _is_dynamic_loader(file):
    return _is_versioned(file, "ld")

def _override_binary_rpath(ctx, path_override, original_binary):
    patched_binary = ctx.actions.declare_file("{}_patched".format(original_binary.path))

    ctx.actions.run(
        inputs = [original_binary],
        outputs = [patched_binary],
        executable = ctx.executable._patchelf,
        arguments = ["--set-rpath", path_override, original_binary.path, "--output", patched_binary.path],
        tools = [],
        mnemonic = "OverrideBinaryRPath",
    )
    return patched_binary

def _set_dynamic_loader(ctx, binary, loader, interpreter_path):
    """Uses provided dynamic loader as the interpreter for the binary"""
    patched_binary = ctx.actions.declare_file("{}_ld".format(binary.path))
    ctx.actions.run(
        inputs = [binary, loader],
        outputs = [patched_binary],
        executable = ctx.executable._patchelf,
        arguments = ["--set-interpreter", "{}/{}".format(interpreter_path, loader.basename), binary.path, "--output", patched_binary.path],
        tools = [],
        mnemonic = "SetDynamicLoader",
    )
    return patched_binary

def _prepare_package_binaries(ctx, binaries, dynamic_loader_path = "/opt/redpanda/lib"):
    """
    Prepares binaries for packaging, collecting shared libraries setting rpath and dynamic loader.

    The function takes the list of binaries containing their attributes and files.
    Returns a struct with two fields:
    - `binary_files`: a list of binary files with their rpath and loader set.
    - `shared_libraries`: a list of shared libraries that were collected from the binaries.
    """

    shared_libraries = []
    dynamic_loader = None
    cc_toolchain = find_cpp_toolchain(ctx)
    if cc_toolchain.sysroot != None and ctx.attr.include_sysroot_libs:
        for cc_file in cc_toolchain.all_files.to_list():
            if cc_file.path.startswith(cc_toolchain.sysroot):
                if _is_versioned_so(cc_file):
                    shared_libraries.append(cc_file)
                elif _is_dynamic_loader(cc_file):
                    shared_libraries.append(cc_file)
                    dynamic_loader = cc_file

    ret_binaries = []
    for binary in binaries:
        if binary.file == None:
            ret_binaries.append(None)
            continue
        runfiles = binary.attr[DefaultInfo].default_runfiles.files.to_list()
        for solib in runfiles:
            # Why the binary is marked as a runfile of itself? No idea...
            if solib == binary.file:
                continue
            shared_libraries.append(solib)
        file = binary.file
        if ctx.attr.rpath_override != "":
            file = _override_binary_rpath(
                ctx,
                ctx.attr.rpath_override,
                file,
            )
        if ctx.attr.include_sysroot_libs:
            file = _set_dynamic_loader(
                ctx,
                file,
                dynamic_loader,
                dynamic_loader_path,
            )
        ret_binaries.append(file)

    return struct(binary_files = ret_binaries, shared_libraries = collections.uniq(shared_libraries))

def _prepare_redpanda_package_content(ctx, dynamic_loader_path = "/opt/redpanda/lib"):
    # Collect all the shared libraries that we built as part of each binary.
    # NOTE: We don't need to do this for `rpk` because it's a static binary,
    # this is only needed for binaries with shared libraries.
    binaries = [
        struct(attr = ctx.attr.redpanda_binary, file = ctx.file.redpanda_binary),
        struct(attr = ctx.attr.rp_util, file = ctx.file.rp_util),
        struct(attr = ctx.attr.iotune, file = ctx.file.iotune),
        struct(attr = ctx.attr.hwloc_calc, file = ctx.file.hwloc_calc),
        struct(attr = ctx.attr.hwloc_distrib, file = ctx.file.hwloc_distrib),
        struct(attr = ctx.attr.openssl, file = ctx.file.openssl),
    ]

    package_binaries = _prepare_package_binaries(
        ctx,
        binaries,
        dynamic_loader_path,
    )

    [rp_binary, rp_util, iotune, hwloc_calc, hwloc_distrib, openssl] = package_binaries.binary_files

    return struct(
        redpanda_binary = rp_binary,
        rp_util = rp_util,
        iotune = iotune,
        hwloc_calc = hwloc_calc,
        hwloc_distrib = hwloc_distrib,
        openssl = openssl,
        rpk_binary = ctx.file.rpk_binary,
        shared_libraries = package_binaries.shared_libraries,
    )

def _common_redpanda_package_cfg(ctx, package_content, fips_enabled, base_path = None):
    def _path(path):
        return "{}/{}".format(base_path, path) if base_path else path

    files = []

    # Redpanda binary is always required.
    files.append({
        "path": _path("bin"),
        "name": "redpanda",
        "source": package_content.redpanda_binary.path,
    })
    if package_content.rp_util != None:
        files.append({
            "path": _path("bin"),
            "name": "rp_util",
            "source": package_content.rp_util.path,
        })

    if package_content.iotune != None:
        files.append({
            "path": _path("bin"),
            "name": "iotune",
            "source": package_content.iotune.path,
        })

    if package_content.hwloc_calc != None:
        files.append({
            "path": _path("bin"),
            "name": "hwloc-calc",
            "source": package_content.hwloc_calc.path,
        })

    if package_content.hwloc_distrib != None:
        files.append({
            "path": _path("bin"),
            "name": "hwloc-distrib",
            "source": package_content.hwloc_distrib.path,
        })

    if package_content.openssl != None:
        files.append({
            "path": _path("bin"),
            "name": "openssl",
            "source": package_content.openssl.path,
        })

    if package_content.rpk_binary != None:
        files.append({
            "path": _path("bin"),
            "name": "rpk",
            "source": package_content.rpk_binary.path,
        })

    for solib in package_content.shared_libraries:
        files.append({
            "path": _path("lib"),
            "name": solib.basename,
            "source": solib.path,
        })

    if fips_enabled:
        files.append({
            "path": _path("fips"),
            "name": "fipsmodule.cnf",
            "source": ctx.file.fips_config.path,
        })
        files.append({
            "path": _path("fips"),
            "name": "fips.so",
            "source": ctx.file.fips_module.path,
        })

    return {
        "package_files": files,
        "package_dirs": [
            _path("bin"),
            _path("lib"),
            _path("config"),
        ],
        "owner": ctx.attr.owner,
    }

def _dir_package_configuration(ctx, package_content, fips_enabled):
    cfg = _common_redpanda_package_cfg(ctx, package_content, fips_enabled)
    cfg["directory_mode"] = True
    if ctx.file.default_yaml_config != None:
        cfg["package_files"].append({
            "path": "config",
            "name": "redpanda.yaml",
            "source": ctx.file.default_yaml_config.path,
        })
    return cfg

def _tarball_package_configuration(ctx, package_content, fips_enabled):
    cfg = _common_redpanda_package_cfg(ctx, package_content, fips_enabled, "opt/redpanda")
    cfg["directory_mode"] = False
    cfg["package_dirs"].append("etc/redpanda")
    cfg["package_dirs"].append("var/lib/redpanda/data")
    if ctx.file.default_yaml_config != None:
        cfg["package_files"].append({
            "path": "/etc/redpanda",
            "name": "redpanda.yaml",
            "source": ctx.file.default_yaml_config.path,
        })
    return cfg

def _impl(ctx):
    use_dir = not ctx.attr.out.endswith(".tar.gz")
    out = ctx.actions.declare_directory(ctx.attr.out) if use_dir else ctx.actions.declare_file(ctx.attr.out)
    interpreter_path = "/opt/redpanda/lib"
    if ctx.attr.install_path:
        interpreter_path = "{}/lib".format(ctx.attr.install_path[BuildSettingInfo].value)
    package_content = _prepare_redpanda_package_content(ctx, interpreter_path)

    fips_enabled = ctx.file.fips_module != None
    if fips_enabled != (ctx.file.fips_config != None):
        fail("`fips_module` and `fips_config` must both be specified in", ctx.attr.name)
    if use_dir:
        cfg = _dir_package_configuration(ctx, package_content, fips_enabled)
    else:
        cfg = _tarball_package_configuration(ctx, package_content, fips_enabled)

    # Create the configuration file for the packaging tool
    cfg_file = ctx.actions.declare_file("%s.config.json" % ctx.attr.name)
    ctx.actions.write(cfg_file, content = json.encode_indent(cfg))

    inputs = [cfg_file, package_content.redpanda_binary] + package_content.shared_libraries

    optional_inputs = [
        package_content.rp_util,
        package_content.iotune,
        package_content.hwloc_calc,
        package_content.hwloc_distrib,
        package_content.openssl,
        package_content.rpk_binary,
        ctx.file.default_yaml_config,
    ]

    for input in optional_inputs:
        if input != None:
            inputs.append(input)

    if fips_enabled:
        inputs.append(ctx.file.fips_module)
        inputs.append(ctx.file.fips_config)

    # run the packaging tool
    ctx.actions.run(
        outputs = [out],
        inputs = inputs,
        tools = [ctx.executable._tool],
        executable = ctx.executable._tool,
        arguments = [
            "-config",
            cfg_file.path,
            "-output",
            out.path,
        ],
        mnemonic = "BuildingRedpandaPackage",
        use_default_shell_env = False,
    )
    return [DefaultInfo(files = depset([out]))]

redpanda_package = rule(
    implementation = _impl,
    attrs = {
        "redpanda_binary": attr.label(
            allow_single_file = True,
            mandatory = True,
        ),
        "rp_util": attr.label(
            allow_single_file = True,
        ),
        "iotune": attr.label(
            allow_single_file = True,
        ),
        "hwloc_calc": attr.label(
            allow_single_file = True,
        ),
        "hwloc_distrib": attr.label(
            allow_single_file = True,
        ),
        "openssl": attr.label(
            allow_single_file = True,
        ),
        "default_yaml_config": attr.label(
            allow_single_file = True,
        ),
        "fips_module": attr.label(
            allow_single_file = True,
        ),
        "fips_config": attr.label(
            allow_single_file = True,
        ),
        "rpk_binary": attr.label(
            allow_single_file = True,
        ),
        "owner": attr.int(),
        "out": attr.string(
            mandatory = True,
        ),
        "include_sysroot_libs": attr.bool(),
        "rpath_override": attr.string(mandatory = False),
        "install_path": attr.label(),
        "_tool": attr.label(
            executable = True,
            allow_files = True,
            cfg = "exec",
            default = Label("//bazel/packaging:tool"),
        ),
        "_cc_toolchain": attr.label(
            default = Label("@bazel_tools//tools/cpp:current_cc_toolchain"),
        ),
        "_patchelf": attr.label(
            executable = True,
            allow_files = True,
            cfg = "exec",
            default = Label("@patchelf"),
        ),
    },
    toolchains = ["@bazel_tools//tools/cpp:toolchain_type"],
)

def _prepapare_package_conent(ctx):
    cc_binaries = [
    ]
    for b in ctx.attr.cc_binaries:
        cc_binaries += [struct(attr = b, file = file) for file in b.files.to_list()]
    install_path_value = ctx.attr.install_path[BuildSettingInfo].value if ctx.attr.install_path else "/opt/{}".format(ctx.attr.name)
    package_cc_binaries = _prepare_package_binaries(
        ctx,
        cc_binaries,
        "{}/lib".format(install_path_value),
    )

    return struct(
        cc_binaries = package_cc_binaries.binary_files,
        shared_libraries = package_cc_binaries.shared_libraries,
    )

def _create_package_config(ctx, package_content):
    package_files = []

    for b in package_content.cc_binaries:
        package_files.append({
            "path": "bin",
            "name": b.basename.removesuffix("_ld").removesuffix("_patched"),
            "source": b.path,
        })

    for sl in package_content.shared_libraries:
        package_files.append({
            "path": "lib",
            "name": sl.basename,
            "source": sl.path,
        })

    return {
        "package_dirs": ["bin", "lib"],
        "directory_mode": True,
        "owner": ctx.attr.owner,  # Default owner
        "package_files": package_files,
    }

def _native_pkg_impl(ctx):
    use_dir = not ctx.attr.out.endswith(".tar.gz")
    out = ctx.actions.declare_directory(ctx.attr.out) if use_dir else ctx.actions.declare_file(ctx.attr.out)
    package_content = _prepapare_package_conent(ctx)
    cfg = _create_package_config(ctx, package_content)
    cfg["directory_mode"] = use_dir

    # Create the configuration file for the packaging tool
    cfg_file = ctx.actions.declare_file("%s.config.json" % ctx.attr.name)
    ctx.actions.write(cfg_file, content = json.encode_indent(cfg))

    inputs = [cfg_file] + package_content.shared_libraries + package_content.cc_binaries

    # run the packaging tool
    ctx.actions.run(
        outputs = [out],
        inputs = inputs,
        tools = [ctx.executable._tool],
        executable = ctx.executable._tool,
        arguments = [
            "-config",
            cfg_file.path,
            "-output",
            out.path,
        ],
        mnemonic = "BuildingRedpandaPackage",
        use_default_shell_env = False,
    )
    return [DefaultInfo(files = depset([out]))]

native_package = rule(
    implementation = _native_pkg_impl,
    attrs = {
        "cc_binaries": attr.label_list(),
        "out": attr.string(
            mandatory = True,
        ),
        "include_sysroot_libs": attr.bool(),
        "rpath_override": attr.string(mandatory = False),
        "owner": attr.int(),
        "install_path": attr.label(),
        "_tool": attr.label(
            executable = True,
            allow_files = True,
            cfg = "exec",
            default = Label("//bazel/packaging:tool"),
        ),
        "_cc_toolchain": attr.label(
            default = Label("@bazel_tools//tools/cpp:current_cc_toolchain"),
        ),
        "_patchelf": attr.label(
            executable = True,
            allow_files = True,
            cfg = "exec",
            default = Label("@patchelf"),
        ),
    },
    toolchains = ["@bazel_tools//tools/cpp:toolchain_type"],
)
