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
from typing import Any
import yaml

CLUSTER_STARTUP_MARKER = "Successfully started Redpanda"
BENCH_START_MARKER = "Starting benchmark traffic"

# Helper script to run PGO training workloads against a RP dev cluster
# We use the devcluster to launch a basic rf=3 cluster and run OMB against it.
# Finally we use llvm-profdata to merge the generated profiles (from all
# brokers) into one file.


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


async def start_dev_cluster(dev_cluster_py: str, redpanda_bin: str, tmpdir: str):
    data_dir = os.path.join(tmpdir, "rp_data")
    os.makedirs(data_dir, exist_ok=True)
    cmd = [
        dev_cluster_py,
        "--cores",
        "2",
        "-d",
        data_dir,
        "--no-use-grafana",
        "--no-use-prometheus",
        "--no-use-minio",
        "-e",
        redpanda_bin,
    ]
    print(f"Launching dev_cluster: {' '.join(cmd)}")
    proc = await asyncio.create_subprocess_exec(
        *cmd,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.STDOUT,
        start_new_session=True,
    )
    return proc


@dataclass
class OmbTarget:
    results_path: str
    total_messages: int


async def start_omb(
    tmpdir: str, omb_benchmark: str
) -> tuple[asyncio.subprocess.Process, OmbTarget]:
    workload_path = (
        f"{os.path.dirname(os.path.realpath(__file__))}/omb_config/workload.yaml"
    )
    driver_path = (
        f"{os.path.dirname(os.path.realpath(__file__))}/omb_config/driver.yaml"
    )
    results_path = os.path.join(tmpdir, "results.json")

    bench_cmd: list[str] = [
        omb_benchmark,
        "--drivers",
        driver_path,
        "--output",
        results_path,
        "--service-version",
        "unknown_version",
        workload_path,
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


async def terminate(proc: asyncio.subprocess.Process, name: str) -> int:
    if not proc:
        return 0
    try:
        print(f"Terminating {name} (pid {proc.pid})")
        os.killpg(proc.pid, signal.SIGINT)
    except ProcessLookupError:
        # process might have already exited (naturally or not)
        pass
    except Exception as e:
        print(f"Error terminating {name}: {e}")
    return await proc.wait()


async def profile(args: argparse.Namespace, tmpdir: str, redpanda_bin: str):
    cluster_proc: asyncio.subprocess.Process | None = None
    omb_proc: asyncio.subprocess.Process | None = None
    cluster_task: asyncio.Task[None] | None = None
    failed = False
    try:
        cluster_proc = await start_dev_cluster(
            args.dev_cluster_py,
            redpanda_bin,
            tmpdir,
        )
        await read_until(cluster_proc, CLUSTER_STARTUP_MARKER, "cluster")
        cluster_task = asyncio.create_task(continue_stream(cluster_proc, "cluster"))

        omb_proc, omb_target = await start_omb(tmpdir, args.omb_benchmark)
        await read_until(omb_proc, BENCH_START_MARKER, "omb")
        await asyncio.create_task(continue_stream(omb_proc, "omb"))
        check_omb(omb_target)

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
    args: argparse.Namespace, base_profile_dir: str, combined_profile_file: str
):
    profiles = glob.glob(f"{base_profile_dir}/*.profraw")

    assert len(profiles) > 0, f"No profiles found in {base_profile_dir}"

    llvm_profdata_cmd: list[str] = [
        args.llvm_profdata_bin,
        "merge",
        "-o",
        combined_profile_file,
        *profiles,
    ]
    print(f"Combining profiles: {' '.join(llvm_profdata_cmd)}")
    subprocess.check_call(llvm_profdata_cmd)


def extra_rp_tar(rp_tar: str, temp_dir: str):
    extract_path = os.path.join(temp_dir, "redpanda_extracted")
    os.makedirs(extract_path)

    with tarfile.open(rp_tar, "r") as tar:
        for member in tar.getmembers():
            tar.extract(member, path=extract_path)

    # Find the redpanda binary (to avoid hardcoding the a little bit brittle path)
    for root, _, files in os.walk(extract_path):
        if "redpanda" in files:
            redpanda_bin = os.path.join(root, "redpanda")
            return redpanda_bin

    raise FileNotFoundError("redpanda binary not found in the tarball")


def main(args: argparse.Namespace):
    with tempfile.TemporaryDirectory(
        prefix="redpanda_pgo_", dir="/dev/shm"
    ) as tmpdirname:
        profile_dir = os.path.join(tmpdirname, "profile_dir")
        os.makedirs(profile_dir)
        os.environ["LLVM_PROFILE_FILE"] = f"{profile_dir}/data-%p.profraw"

        redpanda_bin = extra_rp_tar(args.redpanda_tar, tmpdirname)

        asyncio.run(profile(args, tmpdirname, redpanda_bin))
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
        type=str,
        help="path to dev_cluster.py",
    )
    parser.add_argument(
        "--llvm-profdata-bin",
        type=str,
        help="path to llvm-profdata binary",
    )
    parser.add_argument(
        "--redpanda-tar",
        type=str,
        help="path to redpanda tarball (bazel packaged with runfiles)",
    )
    parser.add_argument(
        "--omb-benchmark",
        type=str,
        help="path to omb benchmark executable",
    )
    parser.add_argument(
        "--combined-profile-file",
        type=str,
        help="output path for combined PGO profile file",
    )
    args = parser.parse_args()
    main(args)
