#!/usr/bin/env python3
import argparse
from dataclasses import dataclass
import glob
import json
import subprocess
import os
import signal
import asyncio
import sys
import tarfile
import tempfile
import time
from typing import Any
import yaml
from pathlib import Path

CLUSTER_STARTUP_MARKER = "Successfully started Redpanda"
BENCH_START_MARKER = "Starting benchmark traffic"

# Helper script to run PGO training workloads against a RP dev cluster
# We use the devcluster to launch a basic rf=3 cluster and run OMB against it.
# Finally we use llvm-profdata to merge the generated profiles (from all
# brokers) into one file.

ICEBERG_SCHEMA = """

syntax = "proto3";

import "google/protobuf/timestamp.proto";

message Simple {
    string name = 1;
    int32 id = 2;
    google.protobuf.Timestamp ts = 3;
}
"""

ICEBERG_SAMPLE_PAYLOAD = b"\n\x1fhello my name is protobuf shady\x10\xb9`\x1a\x0b\x08\xf4\xf4\xb7\xcb\x06\x10\xc0\xb1\xc3v"
ICEBERG_TOPIC_NAME = "iceberg-topic"


async def setup_iceberg_schema_registry_and_topic(
    args: argparse.Namespace, tmpdir: Path
):
    schema_path = tmpdir / "iceberg_schema.proto"
    schema_path.write_text(ICEBERG_SCHEMA)
    schema_create_args: list[str] = [
        str(args.rpk_binary),
        "registry",
        "schema",
        "create",
        f"{ICEBERG_TOPIC_NAME}-value",
        "--schema",
        str(schema_path),
    ]
    proc = await asyncio.create_subprocess_exec(
        *schema_create_args,
        start_new_session=True,
    )
    await proc.wait()
    if proc.returncode != 0:
        raise RuntimeError("Failed to create iceberg schema in schema registry")
    topic_create_args: list[str] = [
        str(args.rpk_binary),
        "topic",
        "create",
        ICEBERG_TOPIC_NAME,
        "-p",
        "18",
        "-r",
        "3",
        "--topic-config=redpanda.iceberg.mode=value_schema_latest",
        "--topic-config=redpanda.iceberg.target.lag.ms=20000",
    ]
    proc = await asyncio.create_subprocess_exec(
        *topic_create_args,
        start_new_session=True,
    )
    await proc.wait()
    if proc.returncode != 0:
        raise RuntimeError("Failed to create iceberg topic")


async def read_until(proc: asyncio.subprocess.Process, marker: str, tag: str):
    while True:
        assert proc.stdout
        line = await proc.stdout.readline()
        if not line:
            raise RuntimeError("EOF reached without readiness phrase")
        line = line.decode("utf8").rstrip()
        print(f"[{tag}] - {line}")
        if marker in line:
            print(f"Detected {tag} startup")
            return


async def continue_stream(proc: asyncio.subprocess.Process, tag: str):
    while True:
        assert proc.stdout
        line = await proc.stdout.readline()
        if not line:
            break
        line = line.decode("utf8").rstrip()
        print(f"[{tag}] - {line}")


async def start_dev_cluster(redpanda_bin: Path, args: argparse.Namespace, tmpdir: Path):
    data_dir = tmpdir / "rp_data"
    data_dir.mkdir(parents=True, exist_ok=True)
    cmd: list[str] = [
        sys.executable,
        str(args.dev_cluster_py),
        "--cores",
        "2",
        "-d",
        str(data_dir),
        "--no-use-grafana",
        "--no-use-prometheus",
        "--minio_executable",
        str(args.minio_binary),
        "-e",
        str(redpanda_bin),
    ]
    print(f"Launching dev_cluster: {' '.join(cmd)}")
    proc = await asyncio.create_subprocess_exec(
        *cmd,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.STDOUT,
        start_new_session=True,
    )
    return proc


def omb_driver_config() -> str:
    config: dict[str, Any] = {
        "name": "pgo-bl3-like",
        "driverClass": "io.openmessaging.benchmark.driver.redpanda.RedpandaBenchmarkDriver",
        "replicationFactor": 3,
        "reset": True,
        "topicConfig": "",
        "commonConfig": (
            "bootstrap.servers=localhost:9092\n"
            "request.timeout.ms=300000\n"
            "security.protocol=PLAINTEXT\n"
        ),
        "producerConfig": (
            "acks=all\nlinger.ms=1\nbatch.size=1\nenable.idempotence=true\n"
        ),
        "consumerConfig": (
            "auto.offset.reset=earliest\n"
            "enable.auto.commit=True\n"
            "max.partition.fetch.bytes=1048576\n"
        ),
    }

    return yaml.dump(config)


def omb_workload_config(tmpdir: Path) -> str:
    payload_file = tmpdir / "payload.pb"
    payload_file.write_bytes(ICEBERG_SAMPLE_PAYLOAD)

    workload: dict[str, Any] = {
        "name": "pgo-bl3-like",
        "messageSize": len(ICEBERG_SAMPLE_PAYLOAD),
        "existingTopicList": [ICEBERG_TOPIC_NAME],
        "payloadFile": str(payload_file),
        "subscriptionsPerTopic": 1,
        "producersPerTopic": 10,
        "consumerPerSubscription": 10,
        "producerRate": 20000,
        "consumerBacklogSizeGB": 0,
        "warmupDurationMinutes": 0,
        "testDurationMinutes": 2,
        "keyDistributor": "NO_KEY",
    }

    return yaml.dump(workload)


@dataclass
class OmbTarget:
    results_path: Path
    total_messages: int


async def start_omb(
    tmpdir: Path, omb_benchmark: Path
) -> tuple[asyncio.subprocess.Process, OmbTarget]:
    tmp_dir = Path(tmpdir) / "omb"
    tmp_dir.mkdir()
    results_path = tmpdir / "results.json"

    driver_config = omb_driver_config()
    driver_path = tmp_dir / "driver.yaml"
    driver_path.write_text(driver_config)
    workload_config = omb_workload_config(tmp_dir)
    workload_path = tmp_dir / "workload.yaml"
    workload_path.write_text(workload_config)

    bench_cmd: list[str] = [
        str(omb_benchmark),
        "--drivers",
        str(driver_path),
        "--output",
        str(results_path),
        "--service-version",
        "unknown_version",
        str(workload_path),
    ]
    print(f"Launching omb: {' '.join(bench_cmd)}")
    proc = await asyncio.create_subprocess_exec(
        *bench_cmd,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.STDOUT,
        start_new_session=True,
    )

    with open(workload_path, "r") as workload_yaml:
        workload = yaml.safe_load(workload_yaml)

    omb_target = OmbTarget(
        results_path=results_path,
        total_messages=workload["producerRate"]
        * (workload["warmupDurationMinutes"] + workload["testDurationMinutes"])
        * 60,
    )
    return proc, omb_target


def check_omb(omb_target: OmbTarget):
    with open(omb_target.results_path, "r") as results_f:
        results = json.load(results_f)

    # OMB will always overshoot but just avoid flakiness
    leeway_factor = 0.95

    if sum(results["sent"]) < omb_target.total_messages * leeway_factor:
        raise RuntimeError("OMB sent too few messages")
    if sum(results["consumed"]) < omb_target.total_messages * leeway_factor:
        raise RuntimeError("OMB consumed too few messages")


async def check_iceberg_state(tmpdir: Path):
    def get_dir_size(path: Path) -> int:
        return sum(f.stat().st_size for f in path.rglob("*") if f.is_file())

    iceberg_dir = (
        tmpdir
        / "rp_data/minio/data/panda-bucket/redpanda-iceberg-catalog/redpanda/iceberg-topic/"
    )
    dlq_dir = (
        tmpdir
        / "rp_data/minio/data/panda-bucket/redpanda-iceberg-catalog/redpanda/iceberg-topic~dlq/"
    )

    def check_sizes() -> str:
        iceberg_dir_size = get_dir_size(iceberg_dir)
        dlq_dir_size = get_dir_size(dlq_dir)

        # We check 1MiB in either direction which gives good enough margin. Can't do
        # exact checks as compression gets involved.
        threshold = 1024 * 1024

        if iceberg_dir_size < threshold:
            return f"Iceberg data directory is unexpectedly small - {iceberg_dir_size}. Probably not enough data was translated"
        if dlq_dir_size > threshold:
            return f"Iceberg DLQ directory is unexpectedly large - {dlq_dir_size}. Probably too much data was failing translation"

        return ""

    start_time = time.time()

    while check_sizes() and (time.time() - start_time) < 120:
        print("Waiting for iceberg data to settle...")
        await asyncio.sleep(1)

    status = check_sizes()
    if status:
        raise RuntimeError(status)


async def terminate(proc: asyncio.subprocess.Process, name: str) -> int:
    try:
        print(f"Terminating {name} (pid {proc.pid})")
        os.killpg(proc.pid, signal.SIGINT)
    except ProcessLookupError:
        # process might have already exited (naturally or not)
        pass
    except Exception as e:
        print(f"Error terminating {name}: {e}")
    return await proc.wait()


async def profile(args: argparse.Namespace, tmpdir: Path, redpanda_bin: Path):
    cluster_proc: asyncio.subprocess.Process | None = None
    omb_proc: asyncio.subprocess.Process | None = None
    cluster_task: asyncio.Task[None] | None = None
    failed = False
    try:
        cluster_proc = await start_dev_cluster(
            redpanda_bin,
            args,
            tmpdir,
        )
        await read_until(cluster_proc, CLUSTER_STARTUP_MARKER, "cluster")
        cluster_task = asyncio.create_task(continue_stream(cluster_proc, "cluster"))

        await setup_iceberg_schema_registry_and_topic(args, tmpdir)

        omb_proc, omb_target = await start_omb(tmpdir, args.omb_benchmark)
        await read_until(omb_proc, BENCH_START_MARKER, "omb")
        await asyncio.create_task(continue_stream(omb_proc, "omb"))
        check_omb(omb_target)
        await check_iceberg_state(tmpdir)

    finally:
        if omb_proc:
            omb_status = await terminate(omb_proc, "omb")
            failed = failed or omb_status != 0
        if cluster_proc:
            cluster_status = await terminate(cluster_proc, "cluster")
            failed = failed or cluster_status != 0
        if cluster_task:
            await cluster_task

    if failed:
        sys.exit(1)


def combine_profiles(
    args: argparse.Namespace, base_profile_dir: Path, combined_profile_file: Path
):
    profiles = glob.glob(f"{base_profile_dir}/*.profraw")

    assert len(profiles) > 0, f"No profiles found in {base_profile_dir}"

    for profile in profiles:
        print(f"Profile: {profile} size: {os.path.getsize(profile)} bytes")

    llvm_profdata_cmd: list[str] = [
        str(args.llvm_profdata_bin),
        "merge",
        "-o",
        str(combined_profile_file),
        *profiles,
    ]
    print(f"Combining profiles: {' '.join(llvm_profdata_cmd)}")
    subprocess.check_call(llvm_profdata_cmd)


def extra_rp_tar(rp_tar: Path, temp_dir: Path):
    extract_path = temp_dir / "redpanda_extracted"
    extract_path.mkdir()

    with tarfile.open(rp_tar, "r") as tar:
        for member in tar.getmembers():
            tar.extract(member, path=extract_path, filter="fully_trusted")

    # Find the redpanda binary (to avoid hardcoding the a little bit brittle path)
    for root, _, files in os.walk(extract_path):
        if "redpanda" in files:
            redpanda_bin = Path(root) / "redpanda"
            return redpanda_bin

    raise FileNotFoundError("redpanda binary not found in the tarball")


def main(args: argparse.Namespace):
    with tempfile.TemporaryDirectory(
        prefix="redpanda_pgo_", dir="/dev/shm"
    ) as tmpdirname:
        tmpdir_path = Path(tmpdirname)
        profile_dir = tmpdir_path / "profile_dir"
        os.makedirs(profile_dir)
        os.environ["LLVM_PROFILE_FILE"] = f"{profile_dir}/data-%p.profraw"

        redpanda_bin = extra_rp_tar(args.redpanda_tar, tmpdir_path)
        asyncio.run(profile(args, tmpdir_path, redpanda_bin))
        combine_profiles(args, profile_dir, args.combined_profile_file)


if __name__ == "__main__":
    # bazel will send us SIGTERM on ctrl-c, so we need to handle it gracefully
    def handler(signum: Any, frame: Any):
        raise KeyboardInterrupt()

    signal.signal(signal.SIGINT, handler)
    signal.signal(signal.SIGTERM, handler)

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--dev-cluster-py",
        type=Path,
        help="path to dev_cluster.py",
    )
    parser.add_argument(
        "--llvm-profdata-bin",
        type=Path,
        help="path to llvm-profdata binary",
    )
    parser.add_argument(
        "--redpanda-tar",
        type=Path,
        help="path to redpanda tarball (bazel packaged with runfiles)",
    )
    parser.add_argument(
        "--omb-benchmark",
        type=Path,
        help="path to omb benchmark executable",
    )
    parser.add_argument(
        "--minio-binary",
        type=Path,
        help="path to minio binary",
    )
    parser.add_argument(
        "--rpk-binary",
        type=Path,
        help="path to rpk binary",
    )
    parser.add_argument(
        "--combined-profile-file",
        type=Path,
        help="output path for combined PGO profile file",
    )
    args = parser.parse_args()
    main(args)
