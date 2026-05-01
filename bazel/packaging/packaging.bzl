"""
A rule to create a redpanda tarball given inputs from the build system.
"""

load("@bazel_skylib//lib:collections.bzl", "collections")

def _patched_file(ctx, original, suffix):
    """Return a new File for patching.

    Based on an input File, declare a new File with a given suffix, in a directory
    named by the target name (so that patched files from different targets do not
    collide."""
    name = "patched/{}/{}_{}".format(ctx.label.name, original.basename, suffix)
    return ctx.actions.declare_file(name)

def _override_binary_rpath(ctx, path_override, original_binary):
    patched_binary = _patched_file(ctx, original_binary, "patched")

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
    patched_binary = _patched_file(ctx, binary, "ld")
    ctx.actions.run(
        inputs = [binary, loader],
        outputs = [patched_binary],
        executable = ctx.executable._patchelf,
        arguments = ["--set-interpreter", "{}/{}".format(interpreter_path, loader.basename), binary.path, "--output", patched_binary.path],
        tools = [],
        mnemonic = "SetDynamicLoader",
    )
    return patched_binary

def _prepare_package_binaries(ctx, binaries, dynamic_loader_path):
    """
    Prepares binaries for packaging, collecting shared libraries setting rpath and dynamic loader.

    The function takes the list of binaries containing their attributes and files.
    Returns a struct with two fields:
    - `binary_files`: a list of binary files with their rpath and loader set.
    - `shared_libraries`: a list of shared libraries that were collected from the binaries.
    """

    shared_libraries = []
    dynamic_loader = None
    if ctx.attr.include_sysroot_libs:
        if not ctx.files.sysroot_runtime:
            fail("include_sysroot_libs is True but sysroot_runtime is not set on", ctx.attr.name)
        loaders = [f for f in ctx.files.sysroot_runtime if f.basename.startswith("ld-linux-")]
        if len(loaders) != 1:
            fail("expected exactly one ld-linux loader in sysroot_runtime, got", [f.basename for f in loaders])
        dynamic_loader = loaders[0]
        shared_libraries.extend(ctx.files.sysroot_runtime)

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

def _prepare_redpanda_package_content(ctx, dynamic_loader_path):
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
    cfg["package_type"] = "directory"
    if ctx.file.default_yaml_config != None:
        cfg["package_files"].append({
            "path": "config",
            "name": "redpanda.yaml",
            "source": ctx.file.default_yaml_config.path,
        })
    return cfg

def _tarball_package_configuration(ctx, package_content, fips_enabled):
    cfg = _common_redpanda_package_cfg(ctx, package_content, fips_enabled, "opt/redpanda")
    cfg["package_type"] = "tarball"
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
    package_content = _prepare_redpanda_package_content(ctx, "{}/lib".format(ctx.attr.install_path))

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
        "sysroot_runtime": attr.label(
            allow_files = True,
            doc = "Sysroot shared libraries plus the glibc dynamic loader. Shipped to install_path/lib; the loader (basename ld-linux-*) is also set as the binaries' interpreter.",
        ),
        "rpath_override": attr.string(mandatory = False),
        "install_path": attr.string(
            default = "/opt/redpanda",
            doc = "The path where the package will be installed, used to set the interpreter path.",
        ),
        "_tool": attr.label(
            executable = True,
            allow_files = True,
            cfg = "exec",
            default = Label("//bazel/packaging:tool"),
        ),
        "_patchelf": attr.label(
            executable = True,
            allow_files = True,
            cfg = "exec",
            default = Label("@patchelf"),
        ),
    },
)

def _prepapare_package_conent(ctx):
    cc_binaries = [
    ]
    for b in ctx.attr.cc_binaries:
        cc_binaries += [struct(attr = b, file = file) for file in b.files.to_list()]

    package_cc_binaries = _prepare_package_binaries(
        ctx,
        cc_binaries,
        "{}/lib".format(ctx.attr.install_path),
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
        "package_type": "directory",
        "owner": ctx.attr.owner,  # Default owner
        "package_files": package_files,
    }

def _native_pkg_impl(ctx):
    use_dir = not ctx.attr.out.endswith(".tar.gz")
    out = ctx.actions.declare_directory(ctx.attr.out) if use_dir else ctx.actions.declare_file(ctx.attr.out)
    package_content = _prepapare_package_conent(ctx)
    cfg = _create_package_config(ctx, package_content)
    cfg["package_type"] = "directory" if use_dir else "tarball"

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
        "include_sysroot_libs": attr.bool(
            default = True,
        ),
        "sysroot_runtime": attr.label(
            allow_files = True,
            doc = "Sysroot shared libraries plus the glibc dynamic loader. Shipped to install_path/lib; the loader (basename ld-linux-*) is also set as the binaries' interpreter.",
        ),
        "rpath_override": attr.string(
            default = "$ORIGIN/../lib",
        ),
        "owner": attr.int(),
        "install_path": attr.string(
            mandatory = True,
            doc = "The path where the package will be installed, used to set the interpreter path.",
        ),
        "_tool": attr.label(
            executable = True,
            allow_files = True,
            cfg = "exec",
            default = Label("//bazel/packaging:tool"),
        ),
        "_patchelf": attr.label(
            executable = True,
            allow_files = True,
            cfg = "exec",
            default = Label("@patchelf"),
        ),
    },
)

def _script_package_impl(ctx):
    out = ctx.actions.declare_directory(ctx.attr.out)

    commands = ["mkdir -p {out}/bin".format(out = out.path)]
    inputs = []

    for target, output_name in ctx.attr.scripts.items():
        files = target.files.to_list()
        if len(files) != 1:
            fail("script_package expected label {} for output '{}' to resolve to exactly one file, got {}".format(
                target.label,
                output_name,
                len(files),
            ))

        f = files[0]
        commands.append("cp {src} {out}/bin/{name}".format(
            src = f.path,
            out = out.path,
            name = output_name,
        ))
        commands.append("chmod +x {out}/bin/{name}".format(
            out = out.path,
            name = output_name,
        ))
        inputs.append(f)

    ctx.actions.run_shell(
        outputs = [out],
        inputs = inputs,
        command = " && ".join(commands),
        mnemonic = "BuildingScriptPackage",
    )
    return [DefaultInfo(files = depset([out]))]

script_package = rule(
    implementation = _script_package_impl,
    attrs = {
        "scripts": attr.label_keyed_string_dict(
            allow_files = True,
            doc = "Map of script source labels to output binary names.",
        ),
        "out": attr.string(
            mandatory = True,
        ),
    },
)

def _prepare_deb_package_content(ctx, dynamic_loader_path):
    # Prepare list of binaries to process
    binaries = []
    binary_map = {}

    if ctx.file.redpanda_binary != None:
        binary_map["redpanda_binary"] = len(binaries)
        binaries.append(struct(attr = ctx.attr.redpanda_binary, file = ctx.file.redpanda_binary))
    if ctx.file.rp_util != None:
        binary_map["rp_util"] = len(binaries)
        binaries.append(struct(attr = ctx.attr.rp_util, file = ctx.file.rp_util))
    if ctx.file.iotune != None:
        binary_map["iotune"] = len(binaries)
        binaries.append(struct(attr = ctx.attr.iotune, file = ctx.file.iotune))
    if ctx.file.hwloc_calc != None:
        binary_map["hwloc_calc"] = len(binaries)
        binaries.append(struct(attr = ctx.attr.hwloc_calc, file = ctx.file.hwloc_calc))
    if ctx.file.hwloc_distrib != None:
        binary_map["hwloc_distrib"] = len(binaries)
        binaries.append(struct(attr = ctx.attr.hwloc_distrib, file = ctx.file.hwloc_distrib))
    if ctx.file.openssl != None:
        binary_map["openssl"] = len(binaries)
        binaries.append(struct(attr = ctx.attr.openssl, file = ctx.file.openssl))

    package_binaries = _prepare_package_binaries(
        ctx,
        binaries,
        dynamic_loader_path,
    )

    return struct(
        redpanda_binary = package_binaries.binary_files[binary_map["redpanda_binary"]] if "redpanda_binary" in binary_map else None,
        rp_util = package_binaries.binary_files[binary_map["rp_util"]] if "rp_util" in binary_map else None,
        iotune = package_binaries.binary_files[binary_map["iotune"]] if "iotune" in binary_map else None,
        hwloc_calc = package_binaries.binary_files[binary_map["hwloc_calc"]] if "hwloc_calc" in binary_map else None,
        hwloc_distrib = package_binaries.binary_files[binary_map["hwloc_distrib"]] if "hwloc_distrib" in binary_map else None,
        openssl = package_binaries.binary_files[binary_map["openssl"]] if "openssl" in binary_map else None,
        rpk_binary = ctx.file.rpk_binary,
        shared_libraries = package_binaries.shared_libraries,
    )

def _deb_package_impl(ctx):
    out = ctx.actions.declare_file(ctx.attr.out)
    package_content = _prepare_deb_package_content(ctx, "{}/lib".format(ctx.attr.install_path))

    package_files = []
    inputs = []

    # Add redpanda binary
    if package_content.redpanda_binary != None:
        package_files.append({
            "path": ctx.attr.install_path + "/bin",
            "name": "redpanda",
            "source": package_content.redpanda_binary.path,
        })
        inputs.append(package_content.redpanda_binary)

    # Add rp_util
    if package_content.rp_util != None:
        package_files.append({
            "path": ctx.attr.install_path + "/bin",
            "name": "rp_util",
            "source": package_content.rp_util.path,
        })
        inputs.append(package_content.rp_util)

    # Add iotune
    if package_content.iotune != None:
        package_files.append({
            "path": ctx.attr.install_path + "/bin",
            "name": "iotune",
            "source": package_content.iotune.path,
        })
        inputs.append(package_content.iotune)

    # Add hwloc-calc (named hwloc-calc-redpanda for rpk compatibility)
    if package_content.hwloc_calc != None:
        package_files.append({
            "path": ctx.attr.install_path + "/bin",
            "name": "hwloc-calc-redpanda",
            "source": package_content.hwloc_calc.path,
        })
        inputs.append(package_content.hwloc_calc)

    # Add hwloc-distrib (named hwloc-distrib-redpanda for rpk compatibility)
    if package_content.hwloc_distrib != None:
        package_files.append({
            "path": ctx.attr.install_path + "/bin",
            "name": "hwloc-distrib-redpanda",
            "source": package_content.hwloc_distrib.path,
        })
        inputs.append(package_content.hwloc_distrib)

    # Add openssl
    if package_content.openssl != None:
        package_files.append({
            "path": ctx.attr.install_path + "/bin",
            "name": "openssl",
            "source": package_content.openssl.path,
        })
        inputs.append(package_content.openssl)

    # Add rpk binary
    if package_content.rpk_binary != None:
        package_files.append({
            "path": ctx.attr.install_path + "/bin",
            "name": "rpk",
            "source": package_content.rpk_binary.path,
        })
        inputs.append(package_content.rpk_binary)

    # Add shared libraries
    for sl in package_content.shared_libraries:
        package_files.append({
            "path": ctx.attr.install_path + "/lib",
            "name": sl.basename,
            "source": sl.path,
        })
        inputs.append(sl)

    scripts = {}
    if ctx.file.preinst != None:
        scripts["preinst"] = ctx.file.preinst.path
        inputs.append(ctx.file.preinst)
    if ctx.file.postinst != None:
        scripts["postinst"] = ctx.file.postinst.path
        inputs.append(ctx.file.postinst)
    if ctx.file.prerm != None:
        scripts["prerm"] = ctx.file.prerm.path
        inputs.append(ctx.file.prerm)
    if ctx.file.postrm != None:
        scripts["postrm"] = ctx.file.postrm.path
        inputs.append(ctx.file.postrm)

    deb_config = {
        "depends": ctx.attr.depends,
        "description": ctx.attr.description,
        "scripts": scripts,
    }
    if ctx.file.systemd_service != None:
        deb_config["systemd_service"] = ctx.file.systemd_service.path
        inputs.append(ctx.file.systemd_service)
    if ctx.file.systemd_slice != None:
        deb_config["systemd_slice"] = ctx.file.systemd_slice.path
        inputs.append(ctx.file.systemd_slice)

    cfg = {
        "name": ctx.attr.package_name,
        "package_type": "deb",
        "package_dirs": ctx.attr.package_dirs,
        "package_symlinks": ctx.attr.package_symlinks,
        "owner": ctx.attr.owner,
        "package_files": package_files,
        "deb": deb_config,
    }

    # Create the configuration file for the packaging tool
    cfg_file = ctx.actions.declare_file("%s.config.json" % ctx.attr.name)
    ctx.actions.write(cfg_file, content = json.encode_indent(cfg))

    inputs = [cfg_file] + inputs

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
        mnemonic = "BuildingDebPackage",
        use_default_shell_env = False,
    )
    return [DefaultInfo(files = depset([out]))]

redpanda_deb_package = rule(
    implementation = _deb_package_impl,
    attrs = {
        "package_name": attr.string(
            mandatory = True,
            doc = "The name of the debian package",
        ),
        "redpanda_binary": attr.label(
            allow_single_file = True,
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
        "rpk_binary": attr.label(
            allow_single_file = True,
        ),
        "out": attr.string(
            mandatory = True,
            doc = "Output .deb file name",
        ),
        "include_sysroot_libs": attr.bool(
            default = True,
        ),
        "sysroot_runtime": attr.label(
            allow_files = True,
            doc = "Sysroot shared libraries plus the glibc dynamic loader. Shipped to install_path/lib; the loader (basename ld-linux-*) is also set as the binaries' interpreter.",
        ),
        "rpath_override": attr.string(
            default = "$ORIGIN/../lib",
        ),
        "owner": attr.int(
            default = 0,
            doc = "Numeric owner ID for package files",
        ),
        "install_path": attr.string(
            default = "/opt/redpanda",
            doc = "The path where the package will be installed",
        ),
        "depends": attr.string_list(
            default = [],
            doc = "List of package dependencies",
        ),
        "description": attr.string(
            default = "Redpanda, the fastest queue in the West",
            doc = "Package description",
        ),
        "preinst": attr.label(
            allow_single_file = True,
            doc = "Preinst maintainer script",
        ),
        "postinst": attr.label(
            allow_single_file = True,
            doc = "Postinst maintainer script",
        ),
        "prerm": attr.label(
            allow_single_file = True,
            doc = "Prerm maintainer script",
        ),
        "postrm": attr.label(
            allow_single_file = True,
            doc = "Postrm maintainer script",
        ),
        "systemd_service": attr.label(
            allow_single_file = True,
            doc = "Systemd service file",
        ),
        "systemd_slice": attr.label(
            allow_single_file = True,
            doc = "Systemd slice file",
        ),
        "package_dirs": attr.string_list(
            default = [],
            doc = "Additional directories to create in the package",
        ),
        "package_symlinks": attr.string_dict(
            default = {},
            doc = "Symlinks to create in the package (destination -> source)",
        ),
        "_tool": attr.label(
            executable = True,
            allow_files = True,
            cfg = "exec",
            default = Label("//bazel/packaging:tool"),
        ),
        "_patchelf": attr.label(
            executable = True,
            allow_files = True,
            cfg = "exec",
            default = Label("@patchelf"),
        ),
    },
)
