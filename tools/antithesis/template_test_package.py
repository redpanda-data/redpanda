#!/usr/bin/python3
# /// script
# requires-python = ">=3.12"
# dependencies = ["jinja2", "pyyaml"]
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
# Package template-based Antithesis tests into config images.
#
# Each directory under tests/antithesis/ containing a test.yml manifest is
# a test. The manifest provides a name (sent as antithesis.test_name, the run's
# name in reports) and default template variables:
#
#   name: <one line>
#   vars:
#     redpanda_image: <default image>
#     ...
#
# The directory is rendered into an Antithesis config directory: every
# *.j2 file is rendered with Jinja2 (using the manifest vars, overridable
# via --var/--redpanda-image), everything else is copied verbatim. The
# result must contain a docker-compose.yaml at its root; it is packaged
# into a FROM scratch config image tagged with a content hash of the
# rendered files, and also written to .antithesis/<test>/ for local runs.
#
# Usage:
#   ./tools/antithesis/template_test_package.py --list
#   ./tools/antithesis/template_test_package.py ct_stress \
#       --redpanda-image my-instrumented-redpanda:abc123
#   ./tools/antithesis/template_test_package.py ct_stress \
#       --var nodes=3 --bootstrap-set write_caching_default=true --submit
#
# With --build-redpanda-image, Redpanda is built with Bazel (instrumented
# via --config=antithesis unless --no-instrumented) and layered over the
# stock image, and the result is used as the redpanda image:
#   ./tools/antithesis/template_test_package.py ct_stress --build-redpanda-image
#

import argparse
import hashlib
import os
import shlex
import shutil
import sys
import tempfile
from pathlib import Path

import yaml
from jinja2 import StrictUndefined, Template
from jinja2.exceptions import UndefinedError

from at_common import (
    add_common_args,
    run,
    submit_test_run,
    validate_common_args,
)

TOOLS_DIR = Path(__file__).resolve().parent
DEPS_DIR = TOOLS_DIR / "template_deps"
REPO_ROOT = TOOLS_DIR.parent.parent
TESTS_ROOT = REPO_ROOT / "tests" / "antithesis"

MANIFEST_NAME = "test.yml"
COMPOSE_NAME = "docker-compose.yaml"
# Relative to the config root. The bootstrap config lives under scripts/ so it
# rides the container's scripts bind mount; the entrypoint stages it into place.
BOOTSTRAP_NAME = "scripts/bootstrap.yaml"

PACKAGE_TARGET = "//bazel/packaging:redpanda_tar"
PACKAGE_TARBALL = "redpanda.tar.gz"
# The tarball lays out INSTALL_ROOT itself and the binaries' ELF interpreter
# is baked to INSTALL_ROOT/lib (install_path on the packaging target), so the
# image must extract the tarball at /.
INSTALL_ROOT = "/opt/redpanda"


def render_template(path: Path, **kwargs) -> str:
    template = Template(
        path.read_text(),
        undefined=StrictUndefined,
        keep_trailing_newline=True,
    )
    return template.render(**kwargs)


def git_tag() -> str:
    sha = run(
        ["git", "rev-parse", "--short=12", "HEAD"], cwd=REPO_ROOT, capture=True
    ).stdout.strip()
    dirty = run(
        ["git", "status", "--porcelain", "--untracked-files=no"],
        cwd=REPO_ROOT,
        capture=True,
    ).stdout.strip()
    return f"{sha}-dirty" if dirty else sha


def build_redpanda_image(args: argparse.Namespace) -> str:
    """Build Redpanda with Bazel and layer it over the base image, keeping
    the stock image conventions (entrypoint, rpk, default config) that the
    compose templates rely on. Returns the built image reference."""
    if not args.skip_bazel_build:
        print("==> Building Redpanda with Bazel")
        cmd = ["bazel", "build", PACKAGE_TARGET]
        if args.instrumented:
            cmd.append("--config=antithesis")
        cmd.extend(shlex.split(args.bazel_args))
        run(cmd, cwd=REPO_ROOT)

    pkg_root = REPO_ROOT / "bazel-bin" / "bazel" / "packaging"
    if not (pkg_root / PACKAGE_TARBALL).exists():
        sys.exit(
            f"Error: {pkg_root / PACKAGE_TARBALL} not found. "
            f"Run without --skip-bazel-build."
        )

    ref = f"redpanda-antithesis:{git_tag()}"
    print(f"==> Building redpanda image: {ref}")
    with tempfile.TemporaryDirectory() as tmpdir:
        dockerfile = Path(tmpdir) / "Dockerfile"
        dockerfile.write_text(
            render_template(
                DEPS_DIR / "redpanda.Dockerfile.j2",
                base_image=args.rp_base_image,
                package_tarball=PACKAGE_TARBALL,
                install_root=INSTALL_ROOT,
            )
        )
        run(
            [
                "docker",
                "build",
                "-f",
                str(dockerfile),
                "--build-context",
                f"packages={pkg_root}",
                "--tag",
                ref,
                tmpdir,
            ]
        )
    return ref


def discover_tests() -> dict[str, Path]:
    return {
        p.parent.name: p.parent for p in sorted(TESTS_ROOT.glob(f"*/{MANIFEST_NAME}"))
    }


def parse_assignments(pairs: list[str], flag: str) -> dict:
    """Parse repeated KEY=VALUE flags; values are parsed as YAML so that
    numbers and booleans keep their type."""
    out = {}
    for pair in pairs:
        key, sep, value = pair.partition("=")
        if not sep or not key:
            sys.exit(f"Error: {flag} expects KEY=VALUE, got {pair!r}")
        out[key] = yaml.safe_load(value) if value else ""
    return out


FAULT_PARAM = {
    "clock_skew": "custom.clock_skew",
    "cpu_mod": "custom.cpu_mod",
    "exclude_from_faults": "custom.exclude_from_faults",
    "node_hang": "custom.include_for_node_hang",
    "node_termination": "custom.include_for_node_termination",
    "node_throttle": "custom.include_for_node_throttle",
}


def fault_params(manifest: dict, tmpl_vars: dict) -> dict[str, str]:
    """Translate the manifest's `faults` block into custom.* launch params.

    Boolean-valued entries (clock_skew, cpu_mod) are global on/off toggles.
    List-valued entries name container roles: `redpanda` expands to every broker
    (redpanda-0..nodes-1), any other entry is a literal container name. Every
    entry is sent explicitly, empty lists included (as ""), so the manifest is
    the single source of truth for the fault posture rather than the webhook
    defaults.
    """
    faults = manifest.get("faults") or {}

    def expand(roles: list[str]) -> list[str]:
        out: list[str] = []
        for role in roles:
            if role == "redpanda":
                nodes = tmpl_vars.get("nodes")
                if nodes is None:
                    sys.exit("Error: faults reference 'redpanda' but 'nodes' is unset")
                out.extend(f"redpanda-{i}" for i in range(int(nodes)))
            else:
                out.append(role)
        return out

    params: dict[str, str] = {}
    for fault, value in faults.items():
        param = FAULT_PARAM.get(fault)
        if param is None:
            sys.exit(
                f"Error: unknown fault {fault!r} in {MANIFEST_NAME} faults "
                f"(known: {', '.join(FAULT_PARAM)})"
            )
        if isinstance(value, bool):
            params[param] = "true" if value else "false"
        else:
            params[param] = " ".join(expand(value))
    return params


def render_tree(src: Path, dst: Path, tmpl_vars: dict) -> None:
    """Render src into dst: *.j2 files are rendered with tmpl_vars (and
    lose the suffix), everything else is copied verbatim. The manifest
    itself is not part of the output."""
    for path in sorted(src.rglob("*")):
        rel = path.relative_to(src)
        if rel == Path(MANIFEST_NAME):
            continue
        target = dst / rel
        if path.is_dir():
            target.mkdir(parents=True, exist_ok=True)
            continue
        target.parent.mkdir(parents=True, exist_ok=True)
        if path.suffix == ".j2":
            try:
                rendered = render_template(path, **tmpl_vars)
            except UndefinedError as e:
                sys.exit(
                    f"Error: rendering {path} failed: {e} "
                    f"(set it in {MANIFEST_NAME} vars or pass --var)"
                )
            target = target.with_suffix("")
            target.write_text(rendered)
            shutil.copymode(path, target)
        else:
            shutil.copy2(path, target)


def content_tag(root: Path) -> str:
    """Deterministic tag derived from the rendered config contents, so a
    given tag always identifies exactly one configuration."""
    digest = hashlib.sha256()
    for path in sorted(p for p in root.rglob("*") if p.is_file()):
        digest.update(str(path.relative_to(root)).encode())
        digest.update(b"\0")
        digest.update(path.read_bytes())
        digest.update(b"\0")
    return digest.hexdigest()[:12]


def is_registry_qualified(image: str) -> bool:
    """True if the image reference names a registry host (docker's own
    heuristic), i.e. it is pullable and does not need to be pushed to the
    Antithesis registry by us."""
    if "/" not in image:
        return False
    first = image.split("/", 1)[0]
    return "." in first or ":" in first or first == "localhost"


def build_config_image(tag: str, config_dir: Path) -> None:
    print(f"==> Building config image: {tag}")
    with tempfile.TemporaryDirectory() as tmpdir:
        dockerfile = Path(tmpdir) / "Dockerfile"
        dockerfile.write_text("FROM scratch\nCOPY . /\n")
        run(
            [
                "docker",
                "build",
                "-f",
                str(dockerfile),
                "--tag",
                tag,
                str(config_dir),
            ]
        )


def push_image(registry: str, local_ref: str) -> str:
    ref = f"{registry}/{local_ref}"
    run(["docker", "tag", local_ref, ref])
    run(["docker", "push", ref])
    return ref


def build_companion_images(
    name: str, test_dir: Path, manifest: dict, slug: str
) -> dict[str, str]:
    """Build the images declared in the manifest's `images:` map.

    Returns {template_var: image_ref}. Each image is a local build tagged
    with a content hash of its build context, so a given tag identifies
    exactly one build. The refs are set as template variables and, for real
    runs, pushed and submitted so Antithesis injects libvoidstar into them.
    """
    built: dict[str, str] = {}
    for var_name, spec in (manifest.get("images") or {}).items():
        context = test_dir / spec["context"]
        if not context.is_dir():
            sys.exit(
                f"Error: test {name} image {var_name}: context {context} does not exist"
            )
        dockerfile = test_dir / spec.get("dockerfile", f"{spec['context']}/Dockerfile")
        short = var_name.removesuffix("_image").replace("_", "-")
        ref = f"{slug}-{short}:{content_tag(context)}"
        print(f"==> Building image {var_name}: {ref}")
        run(["docker", "build", "-f", str(dockerfile), "-t", ref, str(context)])
        built[var_name] = ref
    return built


def package_test(name: str, test_dir: Path, args: argparse.Namespace) -> None:
    print(f"==> Packaging test: {name}")

    manifest = yaml.safe_load((test_dir / MANIFEST_NAME).read_text()) or {}
    tmpl_vars: dict = {"test_name": name, "test_slug": name.replace("_", "-")}
    tmpl_vars.update(manifest.get("vars") or {})
    tmpl_vars.update(parse_assignments(args.var, "--var"))
    if args.redpanda_image:
        tmpl_vars["redpanda_image"] = args.redpanda_image
    bootstrap_overrides = parse_assignments(args.bootstrap_set, "--bootstrap-set")

    companion = build_companion_images(name, test_dir, manifest, tmpl_vars["test_slug"])
    tmpl_vars.update(companion)

    with tempfile.TemporaryDirectory() as tmpdir:
        staging = Path(tmpdir) / "config"
        staging.mkdir()
        render_tree(test_dir, staging, tmpl_vars)

        if not (staging / COMPOSE_NAME).exists():
            sys.exit(f"Error: test {name} does not produce a {COMPOSE_NAME}")

        if bootstrap_overrides:
            bootstrap_path = staging / BOOTSTRAP_NAME
            if not bootstrap_path.exists():
                sys.exit(
                    f"Error: --bootstrap-set given but test {name} "
                    f"has no {BOOTSTRAP_NAME}"
                )
            # Note: rewriting drops the comments from the rendered file.
            bootstrap = yaml.safe_load(bootstrap_path.read_text()) or {}
            bootstrap.update(bootstrap_overrides)
            bootstrap_path.write_text(yaml.safe_dump(bootstrap, sort_keys=False))

        tag = args.tag or content_tag(staging)
        config_ref = f"{tmpl_vars['test_slug']}-config:{tag}"
        build_config_image(config_ref, staging)

        # Keep a rendered copy for local runs.
        out_dir = REPO_ROOT / ".antithesis" / name
        if out_dir.exists():
            shutil.rmtree(out_dir)
        out_dir.parent.mkdir(parents=True, exist_ok=True)
        shutil.copytree(staging, out_dir)

    # The config image and companion images are always local builds that need
    # pushing; the redpanda image only if it is a local build (registry-
    # qualified references are pullable as-is).
    local_refs = [config_ref, *companion.values()]
    rp_image = tmpl_vars.get("redpanda_image", "")
    if rp_image and not is_registry_qualified(rp_image):
        local_refs.append(rp_image)

    pushed: dict[str, str] = {}
    if not args.skip_registry_upload:
        for ref in local_refs:
            pushed[ref] = push_image(args.registry, ref)

    if args.submit:
        submit_test_run(
            password=os.environ["AT_PASSWORD"],
            test_name=manifest.get("name") or name,
            description=args.description or None,
            source=args.source or None,
            ephemeral=args.ephemeral,
            duration_min=args.duration,
            config_image=pushed[config_ref],
            images=[v for k, v in pushed.items() if k != config_ref],
            recipients=[r for r in args.recipients.split(";") if r] or None,
            extra_params=fault_params(manifest, tmpl_vars),
        )

    compose_path = out_dir / COMPOSE_NAME
    lines = [
        "",
        f"Test {name} packaged:",
        f"  config image:   {config_ref}",
        f"  redpanda image: {rp_image}",
    ]
    if pushed:
        lines.append("Pushed:")
        lines.extend(f"  {ref}" for ref in pushed.values())
    else:
        lines.append("Push skipped (--skip-registry-upload); to push manually:")
        for ref in local_refs:
            lines.append(f"  docker tag {ref} {args.registry}/{ref}")
            lines.append(f"  docker push {args.registry}/{ref}")
    lines += [
        "Run locally:",
        f"  docker compose -f {compose_path} up -d",
        f"  docker compose -f {compose_path} down -v",
        "",
    ]
    print("\n".join(lines))


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Package template-based Antithesis tests "
        f"(directories with a {MANIFEST_NAME} under {TESTS_ROOT})."
    )
    parser.add_argument(
        "tests",
        nargs="*",
        help="Names of the tests to package (default: all)",
    )
    parser.add_argument(
        "--list",
        action="store_true",
        help="List the available tests and exit",
    )
    parser.add_argument(
        "--redpanda-image",
        default="",
        help="Override the redpanda_image template variable",
    )
    parser.add_argument(
        "--build-redpanda-image",
        action="store_true",
        help="Build Redpanda with Bazel and package it as the redpanda "
        "image (used as the redpanda_image template variable)",
    )
    parser.add_argument(
        "--rp-base-image",
        default="ubuntu:22.04",
        help="Base image the Bazel-built Redpanda is layered onto. Must match "
        "the Bazel sysroot glibc (Ubuntu 22.04) so the Antithesis-injected "
        "libvoidstar loads; a newer base splits glibc versions and breaks it.",
    )
    parser.add_argument(
        "--instrumented",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Build with --config=antithesis (default: enabled)",
    )
    parser.add_argument(
        "--bazel-args",
        default="",
        help="Extra arguments passed to bazel build",
    )
    parser.add_argument(
        "--skip-bazel-build",
        action="store_true",
        help="Skip building Redpanda (use existing Bazel artifacts)",
    )
    parser.add_argument(
        "--var",
        action="append",
        default=[],
        metavar="KEY=VALUE",
        help="Override a template variable (repeatable; values are parsed as YAML)",
    )
    parser.add_argument(
        "--bootstrap-set",
        action="append",
        default=[],
        metavar="KEY=VALUE",
        help=f"Override a cluster property in the rendered {BOOTSTRAP_NAME} "
        "(repeatable; values are parsed as YAML)",
    )
    parser.add_argument(
        "--tag",
        default="",
        help="Image tag (default: content hash of the rendered config)",
    )
    parser.add_argument(
        "--source",
        default="",
        help="antithesis.source: groups property history across runs. Use a "
        "stable key such as the git branch; runs sharing a source share "
        "history. Required for --no-ephemeral runs.",
    )
    parser.add_argument(
        "--ephemeral",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Keep the run out of the findings history (antithesis.is_ephemeral). "
        "Default: ephemeral, for ad-hoc runs. Pass --no-ephemeral for a "
        "persistent run recorded to history (requires --source).",
    )
    add_common_args(parser)
    args = parser.parse_args()

    validate_common_args(parser, args)

    if args.submit and not args.ephemeral and not args.source:
        parser.error(
            "--no-ephemeral runs must set --source; use a stable grouping key "
            'such as the git branch, e.g. --source "$(git branch --show-current)"'
        )

    available = discover_tests()
    if args.list:
        for name, path in available.items():
            manifest = yaml.safe_load((path / MANIFEST_NAME).read_text()) or {}
            print(f"{name}: {manifest.get('name', '')}")
        return
    if not available:
        sys.exit(f"Error: no tests found under {TESTS_ROOT}")

    selected = args.tests or list(available)
    unknown = [t for t in selected if t not in available]
    if unknown:
        parser.error(
            f"unknown tests: {', '.join(unknown)} (available: {', '.join(available)})"
        )

    if args.submit and not os.environ.get("AT_PASSWORD"):
        sys.exit("Error: --submit requires the AT_PASSWORD environment variable")

    if args.build_redpanda_image:
        if args.redpanda_image:
            parser.error(
                "--redpanda-image and --build-redpanda-image are mutually exclusive"
            )
        args.redpanda_image = build_redpanda_image(args)

    for name in selected:
        package_test(name, available[name], args)


if __name__ == "__main__":
    main()
