#!/usr/bin/python3
# /// script
# requires-python = ">=3.12"
# dependencies = ["jinja2"]
# ///
#
# ==================================================================
# Copyright 2026 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0
# ==================================================================
#
# Package Bazel-built C++ binaries into Antithesis-compatible Docker
# images. Supports single targets, multiple targets, and Bazel patterns.
# Builds Docker images directly using named build contexts, then uploads
# the workload/config images to the registry (unless --skip-registry-upload)
# and optionally launches an Antithesis test run (--submit; reads the API
# password from $AT_PASSWORD).
#
# Usage:
#   # Single target:
#   ./tools/antithesis/single_binary_test_package.py \
#       //src/v/lsm/db/tests:db_bench \
#       --binary-args='--smp 1 --num 1000 --benchmarks mixedworkload --verify'
#
#   # Bazel pattern (all cc_test/cc_binary in a package):
#   ./tools/antithesis/single_binary_test_package.py \
#       //src/v/cluster/tests/...
#
#   # Without Antithesis instrumentation:
#   ./tools/antithesis/single_binary_test_package.py \
#       //src/v/lsm/db/tests:db_bench --no-instrumented
#

import argparse
import functools
import re
import shlex
import shutil
import sys
import tempfile
import xml.etree.ElementTree as ET
from dataclasses import dataclass, field
from pathlib import Path

from at_common import (
    REPO_ROOT,
    add_build_args,
    add_common_args,
    build_config_image,
    docker_build,
    maybe_submit,
    registry_help_str,
    render_template,
    run as _run,
    tag_images,
    upload_images,
    validate_common_args,
)


DEPS_DIR = Path(__file__).resolve().parent / "single_binary_deps"

# bazel and docker commands in this script must run from the repo root.
run = functools.partial(_run, cwd=REPO_ROOT)

_BASE_BAZEL_ARGS = [
    "--@seastar//:shuffle_task_queue=true",
    "--@seastar//:debug=True",
]

_BASE_SEASTAR_ARGS = [
    "--blocked-reactor-notify-ms 2000000",
    "--abort-on-seastar-bad-alloc",
    "--overprovisioned",
]

_EXCLUDED_ENV = {
    "REDPANDA_RNG_SEEDING_MODE_DEFAULT",
    "REDPANDA_RNG_SEEDING_MODE",
}

INSTALL_PREFIX = "/opt/antithesis"
DATA_DIR = f"{INSTALL_PREFIX}/data"
LIB_DIR = f"{INSTALL_PREFIX}/lib"
DRIVER_DIR = f"{INSTALL_PREFIX}/test/v1/single_binary_tests"

_ROOTPATH_RE = r"\$\(rootpath\s+([^)]+)\)"


@dataclass
class TargetInfo:
    rule_kind: str
    args: list[str]
    env: dict[str, str]
    uses_seastar: bool


@dataclass
class BinaryInfo:
    label: str
    name: str
    runtime_args: list[str] = field(default_factory=list)
    env: dict[str, str] = field(default_factory=dict)
    data_files: dict[str, Path] = field(default_factory=dict)


@functools.cache
def _cquery_file(label: str, extra_args: tuple[str, ...] = ()) -> str:
    """Resolve a bazel label to its output file path.

    For built targets, uses cquery to get the output path. For source
    files (which cquery can't resolve), falls back to locating the file
    directly in the workspace.
    """
    result = run(
        ["bazel", "cquery", "--output=files", *extra_args, label],
        capture=True,
        check=False,
    )
    if result.returncode == 0 and result.stdout.strip():
        return result.stdout.strip().splitlines()[-1]
    # Source file — resolve from workspace. Labels like
    # ":foo" or "//pkg:foo" map to <pkg>/<name> in the repo.
    pkg, name = parse_bazel_target(label)
    src_path = pkg + "/" + name if pkg else name
    if not (REPO_ROOT / src_path).exists():
        sys.exit(f"Error: cannot resolve label {label}")
    return src_path


def resolve_rootpaths(
    env: dict[str, str],
    pkg: str,
    extra_bazel_args: tuple[str, ...] = (),
) -> tuple[dict[str, str], dict[str, Path]]:
    """Resolve $(rootpath <label>) references in env var values.

    Uses the full bazel-relative path as the destination under DATA_DIR
    to avoid collisions when different labels resolve to files with the
    same basename.
    """
    resolved: dict[str, str] = {}
    data_files: dict[str, Path] = {}

    for key, value in env.items():
        labels = re.findall(_ROOTPATH_RE, value)
        if not labels:
            resolved[key] = value
            continue

        new_value = value
        for label in labels:
            # Qualify relative labels (e.g. ":foo") with the target's package.
            qualified = f"//{pkg}:{label[1:]}" if label.startswith(":") else label
            rel_path = _cquery_file(qualified, extra_bazel_args)
            local_path = REPO_ROOT / rel_path
            new_value = new_value.replace(
                f"$(rootpath {label})", f"{DATA_DIR}/{rel_path}"
            )
            data_files[rel_path] = local_path.resolve()

        resolved[key] = new_value

    return resolved, data_files


def parse_bazel_target(target: str) -> tuple[str, str]:
    if ":" in target:
        pkg, name = target.split(":", 1)
    else:
        pkg = target
        name = target.rsplit("/", 1)[-1]
    return pkg.lstrip("/"), name


# If a target deps on one of these we assume its safe to pass
# seastar args to it.
_SEASTAR_TEST_DEPS = [
    "//src/v/test_utils:gtest",
    "@seastar//:testing",
]


def query_seastar_targets(patterns: list[str]) -> set[str]:
    """Return the subset of targets matching patterns that use the Seastar test harness."""
    pattern_set = " ".join(patterns)
    dep_set = " ".join(_SEASTAR_TEST_DEPS)
    query_expr = f"rdeps(set({pattern_set}), set({dep_set}))"
    result = run(["bazel", "query", query_expr], capture=True)
    return set(result.stdout.strip().splitlines())


def resolve_and_query_targets(
    patterns: list[str],
    tests_only: bool = False,
) -> tuple[list[str], dict[str, TargetInfo]]:
    """Resolve patterns and query target info in a single bazel query."""
    kind_filter = "cc_test" if tests_only else "cc_test|cc_binary"
    query_expr = f"kind('{kind_filter}', set(" + " ".join(patterns) + "))"
    print("==> Resolving and querying targets")
    result = run(["bazel", "query", "--output=xml", query_expr], capture=True)

    root = ET.fromstring(result.stdout)
    rules = root.findall(".//rule")
    targets = [rule.get("name", "") for rule in rules]
    seastar_targets = query_seastar_targets(patterns) if targets else set()

    info: dict[str, TargetInfo] = {}
    for rule in rules:
        label = rule.get("name", "")

        args_elem = rule.find("list[@name='args']")
        rule_args = [
            s.get("value", "").replace("'", "")
            for s in (args_elem if args_elem is not None else [])
            if s.get("value")
        ]

        env: dict[str, str] = {}
        env_elem = rule.find("dict[@name='env']")
        for pair in env_elem if env_elem is not None else []:
            strings = pair.findall("string")
            if len(strings) == 2 and strings[0].get("value"):
                env[strings[0].get("value", "")] = strings[1].get("value", "")

        info[label] = TargetInfo(
            rule_kind=rule.get("class", "cc_binary"),
            args=rule_args,
            env=env,
            uses_seastar=label in seastar_targets,
        )

    if not targets:
        sys.exit("Error: no targets resolved from the given patterns")
    print(f"    Resolved {len(targets)} target(s)")
    return targets, info


def _flatten_args(args: list[str]) -> list[str]:
    """Flatten multi-token entries (e.g. "--flag value") into individual
    argv elements so each can be independently shell-quoted in the
    driver script."""
    return [tok for arg in args for tok in arg.split()]


def build_runtime_args(
    ti: TargetInfo,
    binary_args: str,
    log_level: str = "",
) -> list[str]:
    # Flatten args which may contain multi-token entries like "--flag value".
    args = _flatten_args(ti.args) if ti.rule_kind == "cc_test" else []
    if ti.uses_seastar:
        for flag in _BASE_SEASTAR_ARGS:
            prefix = flag.split()[0] if " " in flag else flag.split("=")[0]
            if not any(a.startswith(prefix) for a in args):
                args.extend(flag.split())
        if log_level:
            args = [a for a in args if not a.startswith("--default-log-level")]
            args.append(f"--default-log-level={log_level}")
    if binary_args:
        args.extend(shlex.split(binary_args))
    return args


def build_targets(
    patterns: list[str],
    extra_bazel_args: list[str],
) -> None:
    """Build all binary targets in a single bazel invocation."""
    print("==> Building targets")
    run(["bazel", "build"] + patterns + extra_bazel_args)


def collect_shared_libs(binaries: list[BinaryInfo], lib_dir: Path) -> None:
    """Collect shared libraries needed by the given binaries into lib_dir.

    Bazel's output tree is entirely symlink-based, and Docker cannot
    follow symlinks in build contexts. This copies the resolved .so
    files into a flat directory using the mangled names the binaries'
    NEEDED entries expect. The shared libs are typically small (~tens
    of MB) so the copy cost is negligible.
    """
    lib_dir.mkdir(parents=True, exist_ok=True)
    seen: set[str] = set()
    for b in binaries:
        pkg, _ = parse_bazel_target(b.label)
        runfiles = REPO_ROOT / "bazel-bin" / pkg / f"{b.name}.runfiles"
        if not runfiles.exists():
            continue
        for so in runfiles.rglob("*.so*"):
            if so.name in seen:
                continue
            seen.add(so.name)
            real = so.resolve()
            if real.exists() and real.is_file():
                shutil.copy2(real, lib_dir / so.name)


def collect_binary_info(
    targets: list[str],
    target_info: dict[str, TargetInfo],
    binary_args: str,
    log_level: str,
    extra_bazel_args: tuple[str, ...] = (),
) -> list[BinaryInfo]:
    """Build BinaryInfo for each target from bazel query results."""
    binaries: list[BinaryInfo] = []
    for target in targets:
        ti = target_info[target]
        pkg, binary_name = parse_bazel_target(target)
        binary_path = REPO_ROOT / "bazel-bin" / pkg / binary_name
        if not binary_path.exists():
            print(f"    Skipping {target} (not built)")
            continue
        env = {k: v for k, v in ti.env.items() if k not in _EXCLUDED_ENV}
        env, data_files = resolve_rootpaths(env, pkg, extra_bazel_args)
        binaries.append(
            BinaryInfo(
                label=target,
                name=binary_name,
                runtime_args=build_runtime_args(ti, binary_args, log_level),
                env=env,
                data_files=data_files,
            )
        )
    return binaries


def build_workload_image(
    binaries: list[BinaryInfo],
    name: str,
) -> str:
    """Build the workload Docker image using named build contexts.
    Returns the built reference."""
    print(f"==> Building workload image: {name}")

    with tempfile.TemporaryDirectory() as tmpdir:
        ctx = Path(tmpdir)

        # Bazel's output tree is all symlinks which Docker can't follow.
        # So for at least the shared libraries we're copying them into a
        # single tmp dir.
        collect_shared_libs(binaries, ctx / "libs")

        (ctx / "drivers").mkdir()
        for b in binaries:
            (ctx / "drivers" / f"singleton_driver_{b.name}.sh").write_text(
                render_template(
                    DEPS_DIR / "singleton_driver.sh.j2",
                    install_prefix=INSTALL_PREFIX,
                    lib_dir=LIB_DIR,
                    binary_name=b.name,
                    runtime_args=b.runtime_args,
                    env=b.env,
                )
            )

        # Collect binary paths relative to bazel-bin for the COPY commands.
        bin_paths: list[str] = []
        for b in binaries:
            pkg, _ = parse_bazel_target(b.label)
            bin_paths.append(f"{pkg}/{b.name}")

        data_contexts: dict[str, Path] = {}
        data_files: list[tuple[str, str, str]] = []
        for b in binaries:
            for rel_path, local_path in b.data_files.items():
                ctx_name = "data_" + re.sub(r"[^a-zA-Z0-9_]", "_", rel_path)
                if ctx_name not in data_contexts:
                    data_contexts[ctx_name] = local_path.parent
                    data_files.append((ctx_name, local_path.name, rel_path))

        (ctx / "Dockerfile").write_text(
            render_template(
                DEPS_DIR / "workload.Dockerfile.j2",
                install_prefix=INSTALL_PREFIX,
                lib_dir=LIB_DIR,
                data_dir=DATA_DIR,
                data_files=data_files,
                bin_paths=bin_paths,
                driver_dir=DRIVER_DIR,
            )
        )

        bazel_bin = REPO_ROOT / "bazel-bin"
        build_ctx_args = [
            "--build-context",
            f"deps={DEPS_DIR}",
            "--build-context",
            f"bins={bazel_bin}",
        ]
        for ctx_name, src_dir in data_contexts.items():
            build_ctx_args += ["--build-context", f"{ctx_name}={src_dir}"]

        return docker_build(name, build_ctx_args, tmpdir)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Package Bazel C++ binaries for Antithesis testing."
    )
    parser.add_argument(
        "targets",
        nargs="+",
        help="Bazel labels or patterns (e.g. //pkg:target //pkg/...)",
    )
    parser.add_argument(
        "--binary-args", default="", help="Extra runtime arguments for all binaries"
    )
    parser.add_argument(
        "--name", default="", help="Image name (default: derived from first target)"
    )
    parser.add_argument(
        "--log-level",
        default="",
        help="Override default log level (e.g. warn, error, info)",
    )
    parser.add_argument(
        "--tests-only",
        action="store_true",
        help="Only package cc_test targets, excluding cc_binary",
    )
    add_build_args(parser)
    add_common_args(parser)
    args = parser.parse_args()

    validate_common_args(parser, args)

    # Resolve patterns and query target info.
    targets, target_info = resolve_and_query_targets(
        args.targets, tests_only=args.tests_only
    )

    _, first_name = parse_bazel_target(targets[0])
    target_name = args.name or first_name

    extra_bazel_args = list(_BASE_BAZEL_ARGS)
    if args.bazel_args:
        extra_bazel_args.extend(shlex.split(args.bazel_args))
    if args.instrumented:
        extra_bazel_args.append("--config=antithesis")

    # Build all targets.
    if not args.skip_bazel_build:
        build_targets(args.targets, extra_bazel_args)

    # Collect per-binary info (args, env, data files).
    binaries = collect_binary_info(
        targets,
        target_info,
        args.binary_args,
        args.log_level,
        tuple(extra_bazel_args),
    )
    if not binaries:
        sys.exit("Error: no binaries found. Run without --skip-bazel-build?")

    # Build workload image.
    workload_ref = build_workload_image(binaries, target_name)

    hostname = target_name.replace("_", "-")

    def compose_with(image: str) -> str:
        return render_template(
            DEPS_DIR / "compose.yaml.j2", image_tag=image, hostname=hostname
        )

    # The config compose references the stable <name>:latest name, so the
    # config image changes only when the environment itself changes, not on
    # every image rebuild. In the Antithesis environment the submitted
    # antithesis.images digest overrides that name (a digest entry is
    # tagged latest there).
    config_ref = build_config_image(
        f"{target_name}-config", compose_with(f"{target_name}:latest")
    )

    # Write a compose pinning the exact built image, so local runs keep
    # running this build regardless of later packagings.
    compose_out = REPO_ROOT / ".antithesis" / target_name
    compose_out.mkdir(parents=True, exist_ok=True)
    (compose_out / "docker-compose.yaml").write_text(compose_with(workload_ref))

    refs = [workload_ref, config_ref]
    aliases = tag_images(refs, args.tag)
    pushed: dict[str, str] = {}
    if not args.skip_registry_upload:
        pushed = upload_images(args.registry, refs)
        upload_images(args.registry, aliases)

    maybe_submit(args, test_name=target_name, pushed=pushed, config_ref=config_ref)

    binary_names = [b.name for b in binaries]
    drivers_list = "\n".join(
        f"  {DRIVER_DIR}/singleton_driver_{n}.sh" for n in binary_names
    )

    print(f"""
Images built:
  workload: {workload_ref}
  config:   {config_ref}

Singleton drivers ({len(binary_names)}):
{drivers_list}

Run locally:
  docker compose -f {compose_out}/docker-compose.yaml up -d
  docker compose -f {compose_out}/docker-compose.yaml exec workload \\
      {DRIVER_DIR}/singleton_driver_<binary>.sh
  docker compose -f {compose_out}/docker-compose.yaml down

{registry_help_str(args.registry, skipped=args.skip_registry_upload, refs=[*refs, *aliases])}
""")


if __name__ == "__main__":
    main()
