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
# Shared helpers for the Antithesis test-packaging scripts
# (ducktape_test_package.py and single_binary_test_package.py).

import argparse
import json
import os
import shlex
import subprocess
import sys
import tempfile
from pathlib import Path

from jinja2 import StrictUndefined, Template

LAUNCH_URL = "https://redpanda.antithesis.com/api/v1/launch/basic_test"

DEFAULT_REGISTRY = "us-central1-docker.pkg.dev/molten-verve-216720/redpanda-repository"

# Antithesis enforces a floor on run length; also our default.
MIN_DURATION_MIN = 5


def add_common_args(parser: argparse.ArgumentParser) -> None:
    """Add the registry-upload and test-submission flags shared by both
    packaging scripts. Pair with validate_common_args after parsing."""
    parser.add_argument(
        "--skip-registry-upload",
        action="store_true",
        help="Skip uploading the built images to the registry",
    )
    parser.add_argument(
        "--registry",
        default=DEFAULT_REGISTRY,
        help="Docker registry to upload images to",
    )
    parser.add_argument(
        "--submit",
        action="store_true",
        help="Launch an Antithesis test run after pushing "
        "(requires the AT_PASSWORD environment variable)",
    )
    parser.add_argument(
        "--description",
        default="",
        help="Antithesis run description (default: the image/test name)",
    )
    parser.add_argument(
        "--duration",
        type=int,
        default=MIN_DURATION_MIN,
        help=f"Antithesis run duration in minutes "
        f"(default and minimum: {MIN_DURATION_MIN})",
    )
    parser.add_argument(
        "--recipients",
        default="",
        help="Semicolon-separated report email recipients (default: none)",
    )


def validate_common_args(
    parser: argparse.ArgumentParser, args: argparse.Namespace
) -> None:
    """Validate the flags added by add_common_args, erroring via the parser
    on contradictory or out-of-range values."""
    if args.submit and args.skip_registry_upload:
        parser.error("--submit requires uploading images (drop --skip-registry-upload)")
    if args.submit and args.duration < MIN_DURATION_MIN:
        parser.error(f"--duration must be at least {MIN_DURATION_MIN} minutes")


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


def render_template(path: Path, **kwargs) -> str:
    """Render a Jinja2 template file. Undefined variables are errors, and
    shquote (usable as a filter or a function) shell-quotes values."""
    template = Template(
        path.read_text(),
        undefined=StrictUndefined,
        keep_trailing_newline=True,
    )
    template.globals["shquote"] = shlex.quote
    template.environment.filters["shquote"] = shlex.quote
    return template.render(**kwargs)


def image_ref(registry: str, name: str) -> str:
    """Fully-qualified :latest reference for an image in the registry."""
    return f"{registry}/{name}:latest"


def upload_images(registry: str, images: dict[str, str]) -> None:
    """Tag and push each built image to the registry.

    images maps the remote image name (without registry or tag) to the
    local image tag to push, e.g. {"redpanda-ducktape-node": "...-node:latest"}.
    Everything is pushed as :latest.
    """
    for remote, local_tag in images.items():
        ref = image_ref(registry, remote)
        run(["docker", "tag", local_tag, ref])
        run(["docker", "push", ref])


def build_config_image(tag: str, compose_content: str) -> None:
    """Package a docker-compose.yaml into a FROM scratch Antithesis config
    image."""
    print(f"==> Building config image: {tag}")
    with tempfile.TemporaryDirectory() as tmpdir:
        tmp = Path(tmpdir)
        (tmp / "docker-compose.yaml").write_text(compose_content)
        (tmp / "Dockerfile").write_text(
            "FROM scratch\nCOPY docker-compose.yaml /docker-compose.yaml\n"
        )
        run(["docker", "build", "--tag", tag, str(tmp)])


def submit_test_run(
    *,
    password: str,
    duration_min: int,
    config_image: str,
    images: list[str],
    test_name: str | None = None,
    description: str | None = None,
    source: str | None = None,
    ephemeral: bool = False,
    recipients: list[str] | None = None,
    extra_params: dict[str, str] | None = None,
) -> None:
    """Launch a basic_test run via the Antithesis API.

    test_name sets antithesis.test_name, the run's name in reports; without it
    runs carry the launch endpoint's built-in name ("Basic Test"). description
    sets antithesis.description, shown in report headers. source sets
    antithesis.source (groups property history across runs); ephemeral sets
    antithesis.is_ephemeral, keeping the run out of that history. config_image
    and images are fully-qualified registry references (the ones produced by
    upload_images). recipients is an optional list of report email addresses.
    extra_params are additional launch parameters (e.g. tenant-custom `custom.*`
    fault-scoping keys) merged into the body.
    """
    params = {
        "antithesis.duration": str(duration_min),
        "antithesis.config_image": config_image,
    }
    if test_name:
        params["antithesis.test_name"] = test_name
    if description:
        params["antithesis.description"] = description
    if source:
        params["antithesis.source"] = source
    if ephemeral:
        params["antithesis.is_ephemeral"] = "true"
    if images:
        params["antithesis.images"] = ";".join(images)
    if recipients:
        params["antithesis.report.recipients"] = ";".join(recipients)
    if extra_params:
        params.update(extra_params)

    body = json.dumps({"params": params})

    print(f"  $ curl --fail -u redpanda:*** -X POST {LAUNCH_URL} -d {body}")
    subprocess.run(
        [
            "curl",
            "--fail",
            "-u",
            f"redpanda:{password}",
            "-X",
            "POST",
            LAUNCH_URL,
            "-d",
            body,
        ],
        check=True,
        text=True,
    )


def maybe_submit(
    args: argparse.Namespace,
    *,
    name: str,
    images: dict[str, str],
    config_key: str,
) -> None:
    """Launch a test run from parsed args when --submit was given.

    name is the fallback run description; images is the {remote: local}
    map already passed to upload_images; config_key names the entry that
    is the Antithesis config image (every other entry is a workload image).
    """
    if not args.submit:
        return
    password = os.environ.get("AT_PASSWORD")
    if not password:
        sys.exit("Error: --submit requires the AT_PASSWORD environment variable")
    submit_test_run(
        password=password,
        description=args.description or name,
        duration_min=args.duration,
        config_image=image_ref(args.registry, config_key),
        images=[image_ref(args.registry, r) for r in images if r != config_key],
        recipients=[r for r in args.recipients.split(";") if r] or None,
    )


def registry_help_str(registry: str, *, skipped: bool, images: dict[str, str]) -> str:
    """Render the closing registry status block for a script's summary:
    a one-line confirmation when images were pushed, or the manual docker
    tag/push commands to push them later when --skip-registry-upload was set."""
    if not skipped:
        return (
            f"Pushed to the Antithesis registry: {registry}\n"
            f"  (re-run with --skip-registry-upload to build without pushing)"
        )
    lines = [
        "Push to the Antithesis registry (skipped via --skip-registry-upload):",
        f"  REGISTRY={registry}",
    ]
    for remote, local_tag in images.items():
        ref = f"$REGISTRY/{remote}:latest"
        lines.append(f"  docker tag {local_tag} {ref}")
        lines.append(f"  docker push {ref}")
    return "\n".join(lines)
