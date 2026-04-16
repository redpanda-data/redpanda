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
# Package Redpanda ducktape tests into Docker images compatible with
# Antithesis testing.
#
# This script:
#   1. Builds Redpanda via Bazel (optionally with --config=antithesis)
#   2. Builds the base test-node Docker image
#   3. Builds the node image (test-node + baked-in Redpanda binaries)
#   4. Builds the runner image (FROM node image + test code, config,
#      singleton driver, entrypoint)
#   5. Builds the config image (FROM scratch, docker-compose.yaml at /)
#
# Usage:
#   ./tools/antithesis/ducktape_test_package.py \
#       --ducktape-args rptest/tests/e2e_shadow_indexing_test.py
#   ./tools/antithesis/ducktape_test_package.py \
#       --ducktape-args rptest/tests/e2e_shadow_indexing_test.py \
#       --nodes 3 --instrumented
#

import argparse
import json
import shlex
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

from jinja2 import Template

TOOLS_DIR = Path(__file__).resolve().parent
DEPS_DIR = TOOLS_DIR / "ducktape_deps"
REPO_ROOT = TOOLS_DIR.parent.parent

# Root for installed binaries, matching tools/dt and RedpandaInstaller.
INSTALL_ROOT = "/opt/redpanda_installs"


def run(
    cmd: list[str],
    *,
    check: bool = True,
    cwd: Path | None = None,
    capture: bool = False,
) -> subprocess.CompletedProcess:
    print(f"  $ {' '.join(str(c) for c in cmd)}")
    return subprocess.run(
        cmd,
        check=check,
        cwd=cwd,
        capture_output=capture,
        text=True,
    )


def render_template(template_path: Path, **kwargs) -> str:
    with open(template_path) as f:
        return Template(f.read()).render(**kwargs)


def generate_cluster_json(nodes: int) -> str:
    cluster_nodes = []
    for i in range(1, nodes + 1):
        hostname = f"rp-{i}"
        cluster_nodes.append(
            {
                "externally_routable_ip": hostname,
                "ssh_config": {
                    "host": hostname,
                    "hostname": hostname,
                    "identityfile": "/root/.ssh/id_rsa",
                    "password": "UNUSED",
                    "port": 22,
                    "user": "root",
                },
            }
        )
    return json.dumps({"nodes": cluster_nodes}, indent=4)


def generate_globals_json(log_level: str) -> str:
    return json.dumps(
        {
            "rp_install_path_root": f"{INSTALL_ROOT}/redpanda",
            "direct_consumer_verifier_root": f"{INSTALL_ROOT}/direct_consumer_verifier",
            "redpanda_log_level": log_level,
            "scale": "local",
            "enable_cov": "OFF",
            "use_xfs_partitions": False,
            "trim_logs": True,
            "random_seed": None,
            "cloud_storage_url_style": "path",
            "node_ready_timeout_min_sec": 600,
        },
        indent=2,
    )


def generate_compose(
    node_image: str,
    runner_image: str,
    nodes: int,
    test_args: str,
    max_parallel: int,
    test_timeout: int,
    disable_faults: bool,
) -> str:
    return render_template(
        DEPS_DIR / "ducktape_compose.yaml.j2",
        node_image=node_image,
        runner_image=runner_image,
        nodes=nodes,
        test_args=test_args,
        max_parallel=max_parallel,
        test_timeout=test_timeout,
        disable_faults=disable_faults,
    )


def build_redpanda(instrumented: bool, extra_bazel_args: list[str]) -> None:
    print("==> Building Redpanda ducktape packages")
    cmd = [
        "bazel",
        "build",
        "//bazel/packaging:ducktape",
        "//bazel/packaging:direct_consumer_verifier_ducktape",
    ]
    if instrumented:
        cmd.append("--config=antithesis")
    cmd.extend(extra_bazel_args)
    run(cmd, cwd=REPO_ROOT)


def build_test_node_image(image_tag: str) -> None:
    print(f"==> Building base test node image: {image_tag}")

    dockerignore_src = REPO_ROOT / "tests" / "docker" / "Dockerfile.dockerignore"
    dockerignore_dst = REPO_ROOT / ".dockerignore"
    shutil.copy(dockerignore_src, dockerignore_dst)

    try:
        run(
            [
                "docker",
                "build",
                "--tag",
                image_tag,
                "--file",
                str(REPO_ROOT / "tests" / "docker" / "Dockerfile"),
                str(REPO_ROOT),
            ]
        )
    finally:
        if dockerignore_dst.exists():
            dockerignore_dst.unlink()


def build_node_image(
    base_image: str, node_tag: str, rp_image: str | None = None
) -> None:
    """Layer Redpanda binaries on top of the test-node image.

    If rp_image is provided, binaries are extracted from that Docker image
    (e.g. a nightly build from Docker Hub) instead of from the local Bazel
    build output.
    """
    print(f"==> Building node image: {node_tag}")

    with tempfile.TemporaryDirectory() as tmpdir:
        tmp = Path(tmpdir)
        (tmp / "Dockerfile").write_text(
            render_template(
                DEPS_DIR / "node.Dockerfile.j2",
                base_image=base_image,
                rp_image=rp_image,
                install_root=INSTALL_ROOT,
            )
        )
        build_ctx_args: list[str] = []
        if not rp_image:
            pkg_root = REPO_ROOT / "bazel-bin" / "bazel" / "packaging"
            for pkg in ("redpanda_ducktape", "direct_consumer_verifier_ducktape"):
                if not (pkg_root / pkg).exists():
                    sys.exit(
                        f"Error: {pkg_root / pkg} not found. "
                        f"Run without --skip-bazel-build or use --rp-image."
                    )
            build_ctx_args += ["--build-context", f"packages={pkg_root}"]
        run(["docker", "build", *build_ctx_args, "--tag", node_tag, tmpdir])


def build_runner_image(
    node_image: str, runner_tag: str, cluster_json: str, globals_json: str
) -> None:
    """Layer test code, config, and driver on top of the node image."""
    print(f"==> Building runner image: {runner_tag}")

    with tempfile.TemporaryDirectory() as tmpdir:
        tmp = Path(tmpdir)

        # Generated config files (only these need a temp dir).
        (tmp / "cluster.json").write_text(cluster_json)
        (tmp / "globals.json").write_text(globals_json)
        (tmp / "Dockerfile").write_text(
            render_template(
                DEPS_DIR / "runner.Dockerfile.j2",
                node_image=node_image,
            )
        )

        run(
            [
                "docker",
                "build",
                "--build-context",
                f"deps={DEPS_DIR}",
                "--build-context",
                f"rptest={REPO_ROOT / 'tests' / 'rptest'}",
                "--tag",
                runner_tag,
                tmpdir,
            ]
        )


def build_config_image(config_tag: str, compose_content: str) -> None:
    """Build a FROM scratch config image with docker-compose.yaml at /."""
    print(f"==> Building config image: {config_tag}")

    with tempfile.TemporaryDirectory() as tmpdir:
        tmp = Path(tmpdir)
        (tmp / "docker-compose.yaml").write_text(compose_content)
        (tmp / "Dockerfile").write_text(
            "FROM scratch\nCOPY docker-compose.yaml /docker-compose.yaml\n"
        )
        run(["docker", "build", "--tag", config_tag, tmpdir])


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Package Redpanda ducktape tests for Antithesis."
    )
    parser.add_argument(
        "--ducktape-args",
        required=True,
        help="Arguments passed to the ducktape CLI "
        "(e.g. rptest/tests/e2e_shadow_indexing_test.py)",
    )
    parser.add_argument(
        "--nodes", type=int, default=3, help="Number of Redpanda nodes (default: 3)"
    )
    parser.add_argument(
        "--name",
        default="redpanda-ducktape",
        help="Base name for images (default: redpanda-ducktape)",
    )
    parser.add_argument(
        "--instrumented",
        action="store_true",
        help="Build with --config=antithesis for coverage",
    )
    parser.add_argument(
        "--max-parallel",
        type=int,
        default=1,
        help="Max parallel ducktape tests (default: 1)",
    )
    parser.add_argument(
        "--test-timeout",
        type=int,
        default=1800000,
        help="Ducktape test timeout in ms (default: 1800000)",
    )
    parser.add_argument(
        "--disable-faults",
        action="store_true",
        help="Disable Antithesis fault injection during tests",
    )
    parser.add_argument(
        "--log-level", default="info", help="Redpanda log level (default: info)"
    )
    parser.add_argument(
        "--bazel-args", default="", help="Extra arguments passed to bazel build"
    )
    parser.add_argument(
        "--rp-image",
        default="",
        help="Use a pre-built Redpanda Docker image instead of building "
        "locally (e.g. docker.redpanda.com/redpandadata/redpanda-nightly:latest)",
    )
    parser.add_argument(
        "--test-node-image",
        default="",
        help="Use a pre-built test-node image instead of building locally "
        "(e.g. docker.redpanda.com/redpandadata/redpanda-test-node:dev-amd64-cache)",
    )
    parser.add_argument(
        "--skip-bazel-build",
        action="store_true",
        help="Skip building Redpanda (use existing artifacts)",
    )
    parser.add_argument(
        "--skip-docker-build",
        action="store_true",
        help="Skip building the base test-node Docker image",
    )
    args = parser.parse_args()

    base_image = args.test_node_image or "vectorized/redpanda-test-node"
    node_tag = f"{args.name}-node:latest"
    runner_tag = f"{args.name}-runner:latest"
    config_tag = f"{args.name}-config:latest"
    extra_bazel_args = shlex.split(args.bazel_args) if args.bazel_args else []

    rp_image = args.rp_image or None
    if not rp_image and not args.skip_bazel_build:
        build_redpanda(args.instrumented, extra_bazel_args)

    if not args.test_node_image and not args.skip_docker_build:
        build_test_node_image(base_image)

    print("==> Generating config files")
    cluster_json = generate_cluster_json(args.nodes)
    globals_json = generate_globals_json(args.log_level)
    compose = generate_compose(
        node_image=node_tag,
        runner_image=runner_tag,
        nodes=args.nodes,
        test_args=args.ducktape_args,
        max_parallel=args.max_parallel,
        test_timeout=args.test_timeout,
        disable_faults=args.disable_faults,
    )

    build_node_image(base_image, node_tag, rp_image=rp_image)
    build_runner_image(node_tag, runner_tag, cluster_json, globals_json)
    build_config_image(config_tag, compose)

    # Write compose file for local testing.
    compose_out = REPO_ROOT / ".antithesis" / args.name
    compose_out.mkdir(parents=True, exist_ok=True)
    (compose_out / "docker-compose.yaml").write_text(compose)

    print(f"""
Images built:
  node:   {node_tag}
  runner: {runner_tag}
  config: {config_tag}

Run locally:
  docker compose -f {compose_out}/docker-compose.yaml up -d
  docker compose -f {compose_out}/docker-compose.yaml exec ducktape-runner \\
      /opt/antithesis/test/v1/ducktape/singleton_driver_ducktape.sh
  docker compose -f {compose_out}/docker-compose.yaml down

Push to Antithesis registry:
  TENANT=<your-tenant-name>
  REGISTRY=us-central1-docker.pkg.dev/molten-verve-216720/$TENANT-repository
  docker tag {node_tag} $REGISTRY/{args.name}-node:latest
  docker push $REGISTRY/{args.name}-node:latest
  docker tag {runner_tag} $REGISTRY/{args.name}-runner:latest
  docker push $REGISTRY/{args.name}-runner:latest
  docker tag {config_tag} $REGISTRY/{args.name}-config:latest
  docker push $REGISTRY/{args.name}-config:latest
""")


if __name__ == "__main__":
    main()
