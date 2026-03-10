"""
Macro for packaging Redpanda C++ test binaries for Antithesis testing.

Usage in a BUILD file:

    load("//bazel/antithesis:antithesis.bzl", "antithesis_test_image")

    antithesis_test_image(
        name = "memtable_test_antithesis",
        binary = ":memtable_test",
    )

This produces:
  - <name>           : the OCI image
  - <name>_load      : loads the image into the local Docker daemon
  - <name>_compose   : the docker-compose.yml file

Run locally:
    bazel run //path:memtable_test_antithesis_load
    docker compose -f bazel-bin/path/<name>_compose/docker-compose.yml up
"""

load("@rules_oci//oci:defs.bzl", "oci_image", "oci_load")
load("//bazel/packaging:packaging.bzl", "native_package")

_BASE_SEASTAR_ARGS = [
    "--blocked-reactor-notify-ms 2000000",
    "--abort-on-seastar-bad-alloc",
    "--overprovisioned",
]

def _prefixed_tar_impl(ctx):
    """Re-wrap a tar under a prefix path for correct OCI layer placement.

    native_package produces tars with bin/ and lib/ at the root. When used
    as an OCI layer this overwrites /bin and /lib from the base image. This
    rule extracts the tar and re-creates it under the given prefix so that
    the contents land at e.g. /opt/antithesis/bin/ instead of /bin/.
    """
    out = ctx.actions.declare_file(ctx.attr.name + ".tar")
    src = ctx.attr.src.files.to_list()[0]
    prefix = ctx.attr.prefix.lstrip("/")
    ctx.actions.run_shell(
        outputs = [out],
        inputs = [src],
        command = """\
set -e
tmpdir=$(mktemp -d)
mkdir -p "$tmpdir/{prefix}"
if echo "{src}" | grep -q '\\.gz$'; then
    gzip -dc "{src}" | tar -xf - -C "$tmpdir/{prefix}"
else
    tar -xf "{src}" -C "$tmpdir/{prefix}"
fi
tar -cf "{out}" --owner=0 --group=0 -C "$tmpdir" "{prefix}"
rm -rf "$tmpdir"
""".format(
            src = src.path,
            prefix = prefix,
            out = out.path,
        ),
    )
    return [DefaultInfo(files = depset([out]))]

_prefixed_tar = rule(
    implementation = _prefixed_tar_impl,
    attrs = {
        "src": attr.label(mandatory = True, allow_files = True),
        "prefix": attr.string(mandatory = True),
    },
)

def _scripts_tar_impl(ctx):
    """Generate a tar layer with entrypoint.sh and singleton_driver."""
    binary_name = ctx.attr.binary_name

    entrypoint = ctx.actions.declare_file(ctx.attr.name + "/entrypoint.sh")
    ctx.actions.write(
        output = entrypoint,
        content = "".join([
            "#!/usr/bin/sh\n",
            "if [ -n \"$ANTITHESIS_OUTPUT_DIR\" ]; then\n",
            "    mkdir -p \"$ANTITHESIS_OUTPUT_DIR\"\n",
            "    printf '{\"antithesis_setup\": {\"status\": \"complete\", \"details\": {\"message\": \"ready\"}}}\\n' \\\n",
            "        >> \"$ANTITHESIS_OUTPUT_DIR/sdk.jsonl\"\n",
            "fi\n",
            "exec sleep infinity\n",
        ]),
        is_executable = True,
    )

    env_lines = []
    for key, val in ctx.attr.env.items():
        if "$(rootpath" not in val and "$(location" not in val:
            env_lines.append('export {}="{}"'.format(key, val))

    args_str = " ".join(ctx.attr.runtime_args)
    exec_line = "exec /opt/antithesis/bin/{} {}".format(binary_name, args_str).rstrip()

    driver_content = "#!/usr/bin/sh\n"
    driver_content += 'export LD_LIBRARY_PATH="/opt/antithesis/lib${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}"\n'
    if env_lines:
        driver_content += "\n".join(env_lines) + "\n"
    driver_content += "\n" + exec_line + "\n"

    driver = ctx.actions.declare_file(
        ctx.attr.name + "/singleton_driver_{}.sh".format(binary_name),
    )
    ctx.actions.write(output = driver, content = driver_content, is_executable = True)

    tar = ctx.actions.declare_file(ctx.attr.name + ".tar")
    ctx.actions.run_shell(
        outputs = [tar],
        inputs = [entrypoint, driver],
        command = """\
set -e
tmpdir=$(mktemp -d)
mkdir -p "$tmpdir/usr/bin"
mkdir -p "$tmpdir/opt/antithesis/test/v1/quickstart"
mkdir -p "$tmpdir/symbols"
cp "{entrypoint}" "$tmpdir/usr/bin/entrypoint.sh"
chmod 755 "$tmpdir/usr/bin/entrypoint.sh"
cp "{driver}" "$tmpdir/opt/antithesis/test/v1/quickstart/{driver_name}"
chmod 755 "$tmpdir/opt/antithesis/test/v1/quickstart/{driver_name}"
ln -s "/opt/antithesis/bin/{binary_name}" "$tmpdir/symbols/{binary_name}"
tar -cf "{tar}" --owner=0 --group=0 -C "$tmpdir" usr opt symbols
rm -rf "$tmpdir"
""".format(
            entrypoint = entrypoint.path,
            driver = driver.path,
            driver_name = driver.basename,
            binary_name = binary_name,
            tar = tar.path,
        ),
    )

    return [DefaultInfo(files = depset([tar]))]

_antithesis_scripts_tar = rule(
    implementation = _scripts_tar_impl,
    attrs = {
        "binary_name": attr.string(mandatory = True),
        "runtime_args": attr.string_list(default = []),
        "env": attr.string_dict(default = {}),
    },
)

def _compose_impl(ctx):
    hostname = ctx.attr.binary_name.replace("_", "-")
    compose = ctx.actions.declare_file("docker-compose.yaml")
    ctx.actions.write(
        output = compose,
        content = """\
services:
  workload:
    image: {image_tag}
    container_name: {hostname}
    hostname: {hostname}
    init: true
""".format(image_tag = ctx.attr.image_tag, hostname = hostname),
    )
    return [DefaultInfo(files = depset([compose]))]

_antithesis_compose = rule(
    implementation = _compose_impl,
    attrs = {
        "binary_name": attr.string(mandatory = True),
        "image_tag": attr.string(mandatory = True),
    },
)

def antithesis_test_image(
        name,
        binary,
        args = None,
        env = {},
        image_tag = None,
        visibility = None):
    """Package a C++ binary into an Antithesis-compatible OCI image.

    Args:
        name: target name
        binary: label of the cc_test or cc_binary to package
        args: runtime args for the binary. If None, uses the base Seastar
              flags. For cc_test targets, pass the test's args to include
              --smp, --memory, log levels, etc.
        env: environment variables to export before running the binary
        image_tag: Docker image tag (default: antithesis/<binary_name>:latest)
        visibility: Bazel visibility
    """
    if ":" in binary:
        binary_name = binary.split(":")[-1]
    else:
        binary_name = binary.split("/")[-1]
    tag = image_tag or "{}:latest".format(binary_name)
    runtime_args = list(args) if args != None else list(_BASE_SEASTAR_ARGS)

    native_package(
        name = name + "_pkg_raw",
        out = name + "_pkg_raw.tar.gz",
        cc_binaries = [binary],
        install_path = "/opt/antithesis",
        testonly = True,
    )

    _prefixed_tar(
        name = name + "_pkg",
        src = ":" + name + "_pkg_raw",
        prefix = "/opt/antithesis",
        testonly = True,
    )

    _antithesis_scripts_tar(
        name = name + "_scripts",
        binary_name = binary_name,
        runtime_args = runtime_args,
        env = env,
        testonly = True,
    )

    _antithesis_compose(
        name = name + "_compose",
        binary_name = binary_name,
        image_tag = tag,
        testonly = True,
        visibility = visibility,
    )

    oci_image(
        name = name,
        base = "@debian12_slim",
        tars = [
            ":" + name + "_pkg",
            ":" + name + "_scripts",
        ],
        entrypoint = ["/usr/bin/sh", "/usr/bin/entrypoint.sh"],
        testonly = True,
        visibility = visibility,
    )

    oci_load(
        name = name + "_load",
        image = ":" + name,
        repo_tags = [tag],
        testonly = True,
        visibility = visibility,
    )

    if ":" in tag:
        repo, version = tag.rsplit(":", 1)
        config_tag = repo + "-config:" + version
    else:
        config_tag = tag + "-config"

    # Config image: docker-compose.yaml at /.
    # Antithesis reads this to know how to orchestrate the workload.
    native.genrule(
        name = name + "_config_tar",
        srcs = [":" + name + "_compose"],
        outs = [name + "_config.tar"],
        cmd = "tar -chf $@ --owner=0 --group=0 -C $$(dirname $<) $$(basename $<)",
        testonly = True,
    )

    oci_image(
        name = name + "_config",
        tars = [":" + name + "_config_tar"],
        os = "linux",
        architecture = "amd64",
        testonly = True,
        visibility = visibility,
    )

    oci_load(
        name = name + "_config_load",
        image = ":" + name + "_config",
        repo_tags = [config_tag],
        testonly = True,
        visibility = visibility,
    )
