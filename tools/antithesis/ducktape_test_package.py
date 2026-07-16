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
#   1. Builds Redpanda via Bazel (with --config=antithesis unless
#      --no-instrumented)
#   2. Builds the base test-node Docker image
#   3. Builds the node image (test-node + baked-in Redpanda binaries)
#   4. Builds the runner image (FROM node image + test code, config,
#      singleton driver, entrypoint)
#   5. Builds the config image (FROM scratch, docker-compose.yaml at /)
#   6. Uploads the node/runner/config images to the registry
#      (unless --skip-registry-upload)
#   7. Optionally launches an Antithesis test run (--submit; reads the
#      API password from $AT_PASSWORD)
#
# Usage:
#   ./tools/antithesis/ducktape_test_package.py \
#       --ducktape-args rptest/tests/e2e_shadow_indexing_test.py
#   ./tools/antithesis/ducktape_test_package.py \
#       --ducktape-args rptest/tests/e2e_shadow_indexing_test.py \
#       --nodes 3 --no-instrumented
#

import argparse
import json
import shutil
import tempfile
from pathlib import Path

from at_common import (
    REPO_ROOT,
    add_build_args,
    add_common_args,
    bazel_build,
    build_config_image,
    docker_build,
    maybe_submit,
    packaging_artifact,
    registry_help_str,
    render_template,
    run,
    tag_images,
    upload_images,
    validate_common_args,
)

TOOLS_DIR = Path(__file__).resolve().parent
DEPS_DIR = TOOLS_DIR / "ducktape_deps"

# Root for installed binaries, matching tools/dt and RedpandaInstaller.
INSTALL_ROOT = "/opt/redpanda_installs"


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


def build_node_image(base_image: str, name: str, rp_image: str | None = None) -> str:
    """Layer Redpanda binaries on top of the test-node image.

    If rp_image is provided, binaries are extracted from that Docker image
    (e.g. a nightly build from Docker Hub) instead of from the local Bazel
    build output. Returns the built reference.
    """
    print(f"==> Building node image: {name}")

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
            pkg_root = packaging_artifact(
                "redpanda_ducktape",
                hint="Run without --skip-bazel-build or use --rp-image.",
            ).parent
            build_ctx_args += ["--build-context", f"packages={pkg_root}"]
        return docker_build(name, build_ctx_args, tmpdir)


def build_runner_image(
    node_image: str, name: str, cluster_json: str, globals_json: str
) -> str:
    """Layer test code, config, and driver on top of the node image.
    Returns the built reference."""
    print(f"==> Building runner image: {name}")

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

        return docker_build(
            name,
            [
                "--build-context",
                f"deps={DEPS_DIR}",
                "--build-context",
                f"rptest={REPO_ROOT / 'tests' / 'rptest'}",
            ],
            tmpdir,
        )


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
        "--skip-docker-build",
        action="store_true",
        help="Skip building the base test-node Docker image",
    )
    add_build_args(parser)
    add_common_args(parser)

    args = parser.parse_args()

    validate_common_args(parser, args)

    base_image = args.test_node_image or "vectorized/redpanda-test-node"

    rp_image = args.rp_image or None
    if not rp_image and not args.skip_bazel_build:
        bazel_build(
            "//bazel/packaging:ducktape",
            instrumented=args.instrumented,
            bazel_args=args.bazel_args,
        )

    if not args.test_node_image and not args.skip_docker_build:
        build_test_node_image(base_image)

    print("==> Generating config files")
    cluster_json = generate_cluster_json(args.nodes)
    globals_json = generate_globals_json(args.log_level)

    node_ref = build_node_image(base_image, f"{args.name}-node", rp_image=rp_image)
    runner_ref = build_runner_image(
        node_ref, f"{args.name}-runner", cluster_json, globals_json
    )

    def compose_with(node_image: str, runner_image: str) -> str:
        return generate_compose(
            node_image=node_image,
            runner_image=runner_image,
            nodes=args.nodes,
            test_args=args.ducktape_args,
            max_parallel=args.max_parallel,
            test_timeout=args.test_timeout,
            disable_faults=args.disable_faults,
        )

    # The config compose references stable <name>:latest names, so the
    # config image changes only when the environment itself changes, not on
    # every image rebuild. In the Antithesis environment the submitted
    # antithesis.images digests override those names (a digest entry is
    # tagged latest there).
    config_ref = build_config_image(
        f"{args.name}-config",
        compose_with(f"{args.name}-node:latest", f"{args.name}-runner:latest"),
    )

    # Write a compose pinning the exact built images, so local runs keep
    # running this build regardless of later packagings.
    compose_out = REPO_ROOT / ".antithesis" / args.name
    compose_out.mkdir(parents=True, exist_ok=True)
    (compose_out / "docker-compose.yaml").write_text(compose_with(node_ref, runner_ref))

    refs = [node_ref, runner_ref, config_ref]
    aliases = tag_images(refs, args.tag)
    pushed: dict[str, str] = {}
    if not args.skip_registry_upload:
        pushed = upload_images(args.registry, refs)
        upload_images(args.registry, aliases)

    maybe_submit(args, test_name=args.name, pushed=pushed, config_ref=config_ref)

    print(f"""
Images built:
  node:   {node_ref}
  runner: {runner_ref}
  config: {config_ref}

Run locally:
  docker compose -f {compose_out}/docker-compose.yaml up -d
  docker compose -f {compose_out}/docker-compose.yaml exec ducktape-runner \\
      /opt/antithesis/test/v1/ducktape/singleton_driver_ducktape.sh
  docker compose -f {compose_out}/docker-compose.yaml down

{registry_help_str(args.registry, skipped=args.skip_registry_upload, refs=[*refs, *aliases])}
""")


if __name__ == "__main__":
    main()
