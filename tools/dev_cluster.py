#!/usr/bin/python3
#
# ==================================================================
# Copyright 2022 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0
# ==================================================================
#
# Start a 3 node cluster via bazel (builds redpanda automatically):
#
#   bazel run --config=fastbuild //tools:dev_cluster -- [dev_cluster args] -- [redpanda args]
#
# Examples:
#
#   bazel run --config=fastbuild //tools:dev_cluster
#   bazel run --config=fastbuild //tools:dev_cluster -- --nodes 1
#   bazel run --config=release //tools:dev_cluster -- --nodes 1 -- --logger-log-level=io=debug
#
import argparse
import asyncio
import dataclasses
import json
import os
import pathlib
from pathlib import Path
import shutil
import signal
import subprocess
import time
from typing import Optional, Any

import aioboto3
import psutil
import yaml

BOOTSTRAP_YAML = ".bootstrap.yaml"


def pathlib_path_representer(dumper: yaml.SafeDumper, path: Path) -> yaml.ScalarNode:
    return dumper.represent_scalar("!Path", str(path))


def get_config_dumper() -> type[yaml.SafeDumper]:
    d = yaml.SafeDumper
    d.add_representer(pathlib.PosixPath, pathlib_path_representer)
    return d


def yaml_dump(*args: Any, **kwargs: Any) -> None:
    yaml.dump(*args, **kwargs, Dumper=get_config_dumper())


@dataclasses.dataclass
class NetworkAddress:
    address: str
    port: int


@dataclasses.dataclass
class PandaproxyConfig:
    pandaproxy_api: NetworkAddress


@dataclasses.dataclass
class SchemaRegistryConfig:
    schema_registry_api: NetworkAddress


@dataclasses.dataclass
class RedpandaConfig:
    data_directory: Path
    rpc_server: NetworkAddress
    advertised_rpc_api: NetworkAddress
    advertised_kafka_api: NetworkAddress
    kafka_api: NetworkAddress
    admin: NetworkAddress
    seed_servers: list[NetworkAddress]
    empty_seed_starts_cluster: bool = False
    rack: Optional[str] = None
    cloud_storage_enabled: bool = False
    iceberg_enabled: bool = False
    cloud_topics_enabled: bool = False
    enable_developmental_unrecoverable_data_corrupting_features: int = int(time.time())
    enable_metrics_reporter: bool = False


@dataclasses.dataclass
class DefaultMinioRedpandaConfig:
    cloud_storage_enabled: bool = True
    cloud_storage_secret_key: str = "minioadmin"
    cloud_storage_access_key: str = "minioadmin"
    cloud_storage_region: str = "panda-region"
    cloud_storage_bucket: str = "panda-bucket"
    cloud_storage_api_endpoint: str = "localhost"
    cloud_storage_api_endpoint_port: int = 9000
    cloud_storage_disable_tls: bool = True
    cloud_storage_backend: str = "aws"
    iceberg_enabled: bool = True
    cloud_topics_enabled: bool = True


@dataclasses.dataclass
class NodeConfig:
    redpanda: RedpandaConfig
    pandaproxy: PandaproxyConfig
    schema_registry: SchemaRegistryConfig


@dataclasses.dataclass
class NodeMetadata:
    config_path: str

    # This is _not_ the node_id, just the index into our array of nodes
    index: int
    cluster_size: int

    # Dictionary of node config properties.
    config_dict: dict[str, Any]


def cpuset_cpu(
    hardware_core_count: int, stride: int, smp: int, node_index: int, core_index: int
) -> int:
    # Map a (node_index, core_index) pair to a physical CPU index using an
    # interleaved layout that spaces assigned CPUs `stride` apart. This is
    # useful for spreading nodes across physical cores so that co-located
    # hyper-thread siblings or NUMA-adjacent cores are left unused between them.
    #
    # Example: 12 CPUs, stride=2, 3 nodes with smp=2
    #
    #   slot: 0  1 | 2  3 | 4  5
    #    cpu: 0  2 | 4  6 | 8 10
    #         n0   | n1   | n2
    #
    # With stride=1 the layout is contiguous, matching the default.
    # Global slot for this node/core pair.
    slot = (node_index * smp) + core_index
    # Number of CPUs available per interleave group. With stride=16 on a
    # 32-CPU machine there are 2 CPUs per group (0,16 / 1,17 / 2,18 / ...).
    slots_per_group = hardware_core_count // stride
    # Which interleave group this slot falls into.
    group = slot // slots_per_group
    # Position within that group.
    index_in_group = slot % slots_per_group
    return group + (index_in_group * stride)


def cpuset_hardware_core_count() -> int:
    try:
        # this will also include offline CPUs (SMT etc.)
        configured_count = int(subprocess.check_output(["nproc", "--all"]))
        if configured_count > 0:
            return int(configured_count)
    except Exception:
        pass

    fallback_count = psutil.cpu_count(logical=True)
    assert fallback_count
    return fallback_count


async def stream_until_eof(
    process: asyncio.subprocess.Process, name: str, stdout: bool, log_path: Path
) -> None:
    assert process.stdout
    with open(log_path, "w") as log_file:
        while True:
            line_bytes = await process.stdout.readline()
            if not line_bytes:
                break
            line = line_bytes.decode("utf8").rstrip()
            if stdout:
                print(f"{name}: {line}")
            log_file.write(f"{line}\n")
            log_file.flush()


def send_signal(
    proc: asyncio.subprocess.Process, sig: signal.Signals, name: str
) -> None:
    try:
        print(f"Sending signal {sig} to {name} (pid {proc.pid})")
        proc.send_signal(sig)
    except ProcessLookupError:
        # Process already exited
        pass


class Minio:
    def __init__(
        self, binary: Path, directory: Path, rp_config: dict[str, Any]
    ) -> None:
        self.binary = binary
        self.directory = directory
        self.stopped = False
        self.rp_cfg = rp_config
        self.process: asyncio.subprocess.Process

    def stop(self) -> None:
        if not self.stopped:
            self.stopped = True
            send_signal(self.process, signal.SIGINT, "minio")

    async def run(self) -> int:
        log_path = self.directory / "minio.log"

        data_dir = self.directory / "data"
        data_dir.mkdir(parents=True, exist_ok=True)

        # minio really wants a $HOME
        home_dir = self.directory / "home"
        home_dir.mkdir(parents=True, exist_ok=True)

        hostname = self.rp_cfg["cloud_storage_api_endpoint"]
        env = dict(
            HOME=home_dir,
            MINIO_DOMAIN=hostname,
            MINIO_REGION_NAME=self.rp_cfg["cloud_storage_region"],
        )
        port = self.rp_cfg["cloud_storage_api_endpoint_port"]
        args = [
            str(self.binary),
            "server",
            "--address",
            f"{hostname}:{port}",
            str(data_dir),
        ]
        print(f"Running: {args}")
        self.process = await asyncio.create_subprocess_exec(
            *args,
            env=env,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.STDOUT,
        )

        await stream_until_eof(self.process, "minio", True, log_path)

        return await self.process.wait()


class Prometheus:
    def __init__(
        self,
        binary: Path,
        directory: Path,
        listen_address: str = "127.0.0.1",
        port: int = 3001,
        redpanda_admin_ports: list[int] = [],
        scrape_interval: str = "5s",
    ) -> None:
        self.binary = binary
        self.directory = directory
        self.stopped = False
        self.listen_address = listen_address
        self.port = port
        self.redpanda_admin_ports = redpanda_admin_ports
        self.scrape_interval = scrape_interval
        self.process: asyncio.subprocess.Process

    def stop(self) -> None:
        if not self.stopped:
            self.stopped = True
            send_signal(self.process, signal.SIGINT, "prometheus")

    async def run(self) -> int:
        log_path = self.directory / "prometheus.log"
        data_dir = self.directory / "data"
        config_file = self.directory / "prometheus.yml"

        data_dir.mkdir(parents=True, exist_ok=True)

        # Create a basic Prometheus configuration
        config: dict[str, Any] = {
            "global": {
                "scrape_interval": self.scrape_interval,
                "evaluation_interval": self.scrape_interval,
            },
            "scrape_configs": [
                {
                    "job_name": "prometheus",
                    "static_configs": [
                        {"targets": [f"{self.listen_address}:{self.port}"]}
                    ],
                },
                {
                    "job_name": "redpanda_internal",
                    "static_configs": [
                        {
                            "targets": [
                                f"{self.listen_address}:{port}"
                                for port in self.redpanda_admin_ports
                            ]
                        }
                    ],
                },
                {
                    "job_name": "redpanda_public",
                    "metrics_path": "/public_metrics",
                    "static_configs": [
                        {
                            "targets": [
                                f"{self.listen_address}:{port}"
                                for port in self.redpanda_admin_ports
                            ],
                        }
                    ],
                },
            ],
        }

        with open(config_file, "w") as f:
            yaml.dump(config, f)

        args = [
            str(self.binary),
            f"--config.file={config_file}",
            f"--storage.tsdb.path={data_dir}",
            f"--web.listen-address={self.listen_address}:{self.port}",
        ]
        print(f"Running: {' '.join(args)}")
        print(f"Prometheus UI available at: http://{self.listen_address}:{self.port}")

        self.process = await asyncio.create_subprocess_exec(
            *args,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.STDOUT,
        )

        await stream_until_eof(self.process, "prometheus", False, log_path)

        return await self.process.wait()


class Grafana:
    def __init__(
        self,
        binary: Path,
        directory: Path,
        port: int,
        prometheus_url: str | None = None,
        scrape_interval: str = "5s",
    ) -> None:
        self.binary = binary
        self.directory = directory
        self.stopped = False
        self.port = port
        self.prometheus_url = prometheus_url
        self.scrape_interval = scrape_interval
        self.process: asyncio.subprocess.Process

    def stop(self) -> None:
        if not self.stopped:
            self.stopped = True
            send_signal(self.process, signal.SIGINT, "grafana")

    async def run(self) -> int:
        log_path = self.directory / "grafana.log"
        grafana_home = self.directory / "home"
        grafana_home.mkdir(parents=True, exist_ok=True)

        # Copy grafana files (conf, public) into grafana_home
        grafana_binary = Path(self.binary).resolve()
        grafana_root = grafana_binary.parent.parent

        # Copy conf and public directories
        for subdir in ["conf", "public"]:
            src = grafana_root / subdir
            dest = grafana_home / subdir
            if src.exists() and not dest.exists():
                shutil.copytree(src, dest)
            elif not src.exists():
                print(f"Warning: Could not find {src}")

        # Configure Prometheus as a datasource via provisioning
        if self.prometheus_url:
            provisioning_dir = grafana_home / "conf" / "provisioning" / "datasources"
            provisioning_dir.mkdir(parents=True, exist_ok=True)

            datasource_config = {
                "apiVersion": 1,
                "datasources": [
                    {
                        "name": "Prometheus",
                        "type": "prometheus",
                        "access": "proxy",
                        "url": self.prometheus_url,
                        "isDefault": True,
                        "editable": True,
                        "jsonData": {
                            "timeInterval": self.scrape_interval,
                        },
                    }
                ],
            }

            datasource_file = provisioning_dir / "prometheus.yml"
            with open(datasource_file, "w") as f:
                yaml.dump(datasource_config, f)
            print(f"Configured Prometheus datasource at {self.prometheus_url}")

            redpanda_root = os.environ.get("BUILD_WORKSPACE_DIRECTORY")
            if redpanda_root:
                dashboards_dir = Path(redpanda_root) / "tools" / "dashboards"
                provisioning_dir = grafana_home / "conf" / "provisioning" / "dashboards"
                provisioning_dir.mkdir(parents=True, exist_ok=True)

                dashboards_config = {
                    "apiVersion": 1,
                    "providers": [
                        {
                            "name": "Imported Dashboards",
                            "folder": "Dashboards",
                            "type": "file",
                            "editable": True,
                            "disableDeletion": False,
                            "updateIntervalSeconds": 1,
                            "options": {
                                "path": dashboards_dir,
                            },
                        }
                    ],
                }

                dashboards_file = provisioning_dir / "dashboards.yml"
                with open(dashboards_file, "w") as f:
                    yaml_dump(dashboards_config, f)

        env = os.environ.copy()
        env["GF_SERVER_HTTP_ADDR"] = "0.0.0.0"
        env["GF_SERVER_HTTP_PORT"] = str(self.port)
        env["GF_SECURITY_ADMIN_PASSWORD"] = "admin"
        env["GF_SECURITY_ADMIN_USER"] = "admin"
        env["GF_AUTH_BASIC_ENABLED"] = "false"
        env["GF_AUTH_DISABLE_LOGIN_FORM"] = "true"
        env["GF_AUTH_ANONYMOUS_ENABLED"] = "true"
        env["GF_AUTH_ANONYMOUS_ORG_ROLE"] = "Admin"
        env["GF_DASHBOARDS_MIN_REFRESH_INTERVAL"] = "1s"

        args = [str(grafana_binary), "server"]
        print(f"Running: {' '.join(args)}")
        print(f"Grafana UI available on port {self.port}")

        self.process = await asyncio.create_subprocess_exec(
            *args,
            env=env,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.STDOUT,
            cwd=grafana_home,
        )

        await stream_until_eof(self.process, "grafana", False, log_path)

        return await self.process.wait()


class Redpanda:
    def __init__(
        self,
        binary: Path,
        cores: int,
        cpuset_stride: int,
        node_meta: NodeMetadata,
        extra_args: list[str],
        env: dict[str, str],
    ) -> None:
        self.binary = binary
        self.cores = cores
        self.cpuset_stride = cpuset_stride
        self.node_meta = node_meta
        self.process: asyncio.subprocess.Process | None = None
        self.extra_args = extra_args
        self.env = env

    def cpuset(self) -> str:
        hardware_core_count = cpuset_hardware_core_count()
        return ",".join(
            str(
                cpuset_cpu(
                    hardware_core_count,
                    self.cpuset_stride,
                    self.cores,
                    self.node_meta.index,
                    core,
                )
            )
            for core in range(self.cores)
        )

    def stop(self) -> None:
        print(f"node-{self.node_meta.index}: dev_cluster stop requested")
        assert self.process
        send_signal(self.process, signal.SIGINT, f"node-{self.node_meta.index}")

    async def run(self) -> int:
        log_path = Path(os.path.dirname(self.node_meta.config_path)) / "redpanda.log"

        def has_arg(*prefixes: str) -> bool:
            """Check if any extra_arg starts with any of the given prefixes."""
            return any(arg.startswith(prefixes) for arg in self.extra_args)

        # If user did not override cores with extra args, apply it from our internal cores setting
        if not has_arg("-c", "--smp"):
            # Caller is required to pass a finite core count
            assert self.cores > 0
            cores_args = f"--cpuset {self.cpuset()}"
        else:
            cores_args = ""

        # If user did not specify memory, share 75% of memory equally between nodes, capped at 4GB
        if not has_arg("-m", "--memory"):
            max_memory_per_node = 4 * 2**30  # 4GB
            memory_total = psutil.virtual_memory().total
            memory_per_node = (3 * (memory_total // 4)) // self.node_meta.cluster_size
            memory_per_node = min(memory_per_node, max_memory_per_node)
            memory_args = f"-m {memory_per_node // (1024 * 1024)}M"
        else:
            memory_args = ""

        extra_args = " ".join(f'"{a}"' for a in self.extra_args)

        self.process = await asyncio.create_subprocess_shell(
            f"{self.binary} --redpanda-cfg {self.node_meta.config_path} {cores_args} {memory_args} {extra_args}",
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.STDOUT,
            env=self.env,
        )

        await stream_until_eof(
            self.process, f"node-{self.node_meta.index}", True, log_path
        )

        return await self.process.wait()


async def run_command(cmd: str) -> bool:
    proc = await asyncio.create_subprocess_shell(
        cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
    )

    stdout, stderr = await proc.communicate()

    print(f"[{cmd!r} exited with {proc.returncode}]")
    if stdout:
        print(f"[{cmd!r}].[stdout]\n{stdout.decode()}")
    if stderr:
        print(f"[{cmd!r}].[stderr]\n{stderr.decode()}")

    return proc.returncode == 0


async def ensure_bucket_exists(cfg: dict[str, Any]) -> None:
    session = aioboto3.Session()
    client = session.client(
        service_name="s3",
        endpoint_url=f"http://{cfg['cloud_storage_api_endpoint']}:{cfg['cloud_storage_api_endpoint_port']}",
        aws_access_key_id=cfg["cloud_storage_access_key"],
        aws_secret_access_key=cfg["cloud_storage_secret_key"],
    )
    print("Preparing cloud storage")
    async with client as s3:
        timeout_sec = 5
        start = time.time()
        while True:
            try:
                buckets = await s3.list_buckets()
                for bucket in buckets["Buckets"]:
                    if bucket["Name"] == cfg["cloud_storage_bucket"]:
                        print("Bucket exists, proceeding to start redpanda")
                        return
                print("Bucket not found, creating...")
                await s3.create_bucket(Bucket=cfg["cloud_storage_bucket"])
            except Exception as e:
                if (time.time() - start) >= timeout_sec:
                    raise e
                await asyncio.sleep(1)


async def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-e",
        "--executable",
        type=Path,
        help="path to redpanda executable",
        default="redpanda",
    )
    parser.add_argument(
        "--ubsan_suppression_file",
        type=Path,
        help="path to ubsan_suppressions.txt",
    )
    parser.add_argument(
        "--lsan_suppression_file",
        type=Path,
        help="path to lsan_suppressions.txt",
    )
    parser.add_argument("--nodes", type=int, help="number of nodes", default=3)
    parser.add_argument(
        "--cores", type=int, help="number of cores per node", default=None
    )
    parser.add_argument(
        "--cpuset-stride",
        type=int,
        help="stride between assigned cpuset CPUs. 1 means no gaps",
        default=1,
    )
    parser.add_argument(
        "-d", "--directory", type=Path, help="data directory", default=None
    )
    parser.add_argument(
        "--delete-data-dir",
        action=argparse.BooleanOptionalAction,
        help="delete the data directory before starting",
        default=False,
    )
    parser.add_argument("--base-rpc-port", type=int, help="rpc port", default=33145)
    parser.add_argument("--base-kafka-port", type=int, help="kafka port", default=9092)
    parser.add_argument("--base-admin-port", type=int, help="admin port", default=9644)
    parser.add_argument(
        "--base-schema-registry-port",
        type=int,
        help="schema registry port",
        default=8081,
    )
    parser.add_argument(
        "--base-pandaproxy-port",
        type=int,
        help="pandaproxy port",
        # We can't use the "normal" pandaproxy port due to conflicts
        default=8092,
    )
    parser.add_argument(
        "--port-offset",
        type=int,
        help="offset to add to all base ports (useful for running multiple clusters)",
        default=0,
    )
    parser.add_argument(
        "--listen-address", type=str, help="listening address", default="127.0.0.1"
    )
    parser.add_argument(
        "--racks",
        dest="racks",
        help="racks for each of node",
        action="append",
        default=None,
    )
    parser.add_argument(
        "-o",
        "--minio_executable",
        type=Path,
        help="path to minio executable",
        default="minio",
    )
    parser.add_argument(
        "--use-minio",
        action=argparse.BooleanOptionalAction,
        help="whether to spin up an instance of minio and use Redpanda configuration presets for it",
        default=True,
    )
    parser.add_argument("--rpk", type=Path, help="path to rpk executable", default=None)
    parser.add_argument(
        "--prometheus",
        type=Path,
        help="path to prometheus executable",
        default=None,
    )
    parser.add_argument(
        "--use-prometheus",
        action=argparse.BooleanOptionalAction,
        help="whether to spin up an instance of prometheus",
        default=True,
    )
    parser.add_argument(
        "--scrape-interval",
        type=str,
        help="prometheus scrape interval (e.g. '1s', '5s', '15s')",
        default="5s",
    )
    parser.add_argument(
        "--grafana",
        type=Path,
        help="path to grafana executable",
        default=None,
    )
    parser.add_argument(
        "--use-grafana",
        action=argparse.BooleanOptionalAction,
        help="whether to spin up an instance of grafana",
        default=True,
    )
    parser.add_argument(
        "--grafana-port",
        type=int,
        help="grafana listening port",
        default=3000,
    )
    parser.add_argument(
        "--config-overrides",
        type=str,
        help="JSON dictionary of config overrides to apply to all nodes",
        default=None,
    )
    args, extra_args = parser.parse_known_args()

    if extra_args and extra_args[0] == "--":
        extra_args = extra_args[1:]
    elif extra_args:
        # Re-do with strict parse: this will surface unknown argument errors
        args = parser.parse_args()

    if args.directory is None:
        args.directory = Path(os.environ.get("BUILD_WORKSPACE_DIRECTORY", ".")) / "data"

    if (
        args.delete_data_dir
        and args.directory.exists()
        # safety check that we are dealing with a dev cluster data dir
        and (args.directory / "node0/config.yaml").exists()
    ):
        print(f"Deleting existing data directory: {args.directory}")
        shutil.rmtree(args.directory)

    # Apply port offset to all base ports
    if args.port_offset:
        args.base_rpc_port += args.port_offset
        args.base_kafka_port += args.port_offset
        args.base_admin_port += args.port_offset
        args.base_schema_registry_port += args.port_offset
        args.base_pandaproxy_port += args.port_offset

    # Use the first 3 nodes as seed servers
    rpc_addresses = [
        NetworkAddress(args.listen_address, args.base_rpc_port + i)
        for i in range(args.nodes)
    ]

    def make_node_metadata(
        i: int, data_dir: Path, config_path: Path, rack: str | None
    ) -> NodeMetadata:
        def make_address(p: int) -> NetworkAddress:
            return NetworkAddress(args.listen_address, p + i)

        rpc_address = rpc_addresses[i]
        redpanda = RedpandaConfig(
            data_directory=data_dir,
            rpc_server=rpc_address,
            advertised_rpc_api=rpc_address,
            advertised_kafka_api=make_address(args.base_kafka_port),
            kafka_api=make_address(args.base_kafka_port),
            admin=make_address(args.base_admin_port),
            seed_servers=rpc_addresses[:3],
            empty_seed_starts_cluster=False,
            rack=rack,
        )

        pandaproxy = PandaproxyConfig(
            pandaproxy_api=make_address(args.base_pandaproxy_port)
        )
        schema_registry = SchemaRegistryConfig(
            schema_registry_api=make_address(args.base_schema_registry_port)
        )
        node_conf = NodeConfig(
            redpanda=redpanda, pandaproxy=pandaproxy, schema_registry=schema_registry
        )
        return NodeMetadata(
            config_path=str(config_path),
            index=i,
            cluster_size=args.nodes,
            config_dict=dataclasses.asdict(node_conf),
        )

    def prepare_node(i: int, rack: str | None) -> NodeMetadata:
        node_dir = args.directory / f"node{i}"
        data_dir = node_dir / "data"
        conf_file = node_dir / "config.yaml"

        node_dir.mkdir(parents=True, exist_ok=True)
        data_dir.mkdir(parents=True, exist_ok=True)

        node_meta = make_node_metadata(i, data_dir, conf_file, rack)
        config_dict = node_meta.config_dict

        if args.use_minio:
            default_minio_rp_config = dataclasses.asdict(DefaultMinioRedpandaConfig())
            config_dict["redpanda"] = config_dict["redpanda"] | default_minio_rp_config

        if args.config_overrides:
            try:
                config_overrides = json.loads(args.config_overrides)
                config_dict["redpanda"] = config_dict["redpanda"] | config_overrides
            except json.JSONDecodeError as e:
                raise ValueError(f"Invalid JSON in config overrides: {e}")

        with open(conf_file, "w") as f:
            yaml_dump(config_dict, f, indent=2)

        # If there is a bootstrap file in pwd, propagate it to each node's
        # directory so that they'll load it on first start
        if os.path.exists(BOOTSTRAP_YAML):
            shutil.copyfile(BOOTSTRAP_YAML, node_dir / BOOTSTRAP_YAML)

        return node_meta

    if args.racks and len(args.racks) != args.nodes:
        raise Exception("Rack must be specified for each node")

    node_metas = [
        prepare_node(i, None if args.racks is None else args.racks[i])
        for i in range(args.nodes)
    ]

    minio = None
    minio_task = None
    if args.use_minio:
        minio_dir = args.directory / "minio"
        minio_dir.mkdir(parents=True, exist_ok=True)
        minio = Minio(
            args.minio_executable, minio_dir, node_metas[0].config_dict["redpanda"]
        )
        minio_task = asyncio.create_task(minio.run())
        await ensure_bucket_exists(node_metas[0].config_dict["redpanda"])

    prometheus = None
    prometheus_task = None
    if args.use_prometheus and args.prometheus:
        prometheus_dir = args.directory / "prometheus"
        prometheus_dir.mkdir(parents=True, exist_ok=True)
        prometheus = Prometheus(
            args.prometheus,
            prometheus_dir,
            args.listen_address,
            redpanda_admin_ports=[args.base_admin_port + i for i in range(args.nodes)],
            scrape_interval=args.scrape_interval,
        )
        prometheus_task = asyncio.create_task(prometheus.run())

    grafana = None
    grafana_task = None
    if args.use_grafana and args.grafana:
        grafana_dir = args.directory / "grafana"
        grafana_dir.mkdir(parents=True, exist_ok=True)

        # Build Prometheus URL if Prometheus is enabled
        prometheus_url = None
        if prometheus:
            prometheus_url = f"http://{prometheus.listen_address}:{prometheus.port}"

        grafana = Grafana(
            args.grafana,
            grafana_dir,
            port=args.grafana_port,
            prometheus_url=prometheus_url,
            scrape_interval=args.scrape_interval,
        )
        grafana_task = asyncio.create_task(grafana.run())

    cores = args.cores
    if cores is None:
        # Use 75% of cores for redpanda.  e.g. 3 node cluster on a 16 node system
        # gives each node 4 cores.
        cpu_count = psutil.cpu_count(logical=False)
        assert cpu_count
        cores = max((3 * (cpu_count // 4)) // args.nodes, 1)
    env = os.environ.copy()
    if "ASAN_OPTIONS" not in env:
        env["ASAN_OPTIONS"] = "disable_coredump=0:abort_on_error=1"
    if "UBSAN_OPTIONS" not in env:
        env["UBSAN_OPTIONS"] = "halt_on_error=1:abort_on_error=1:report_error_type=1"
        if args.ubsan_suppression_file:
            env["UBSAN_OPTIONS"] += f":suppressions={args.ubsan_suppression_file}"
    if args.lsan_suppression_file and "LSAN_OPTIONS" not in env:
        env["LSAN_OPTIONS"] = f"suppressions={args.lsan_suppression_file}"
    nodes = [
        Redpanda(
            args.executable,
            cores,
            args.cpuset_stride,
            m,
            extra_args,
            env,
        )
        for m in node_metas
    ]

    all_coros = [r.run() for r in nodes]

    def stop() -> None:
        for n in nodes:
            n.stop()
        if minio:
            minio.stop()
        if prometheus:
            prometheus.stop()
        if grafana:
            grafana.stop()

    asyncio.get_event_loop().add_signal_handler(signal.SIGINT, stop)

    def failed_exit_code(rc: int) -> bool:
        # ignore -2/SIGINT, natural way to stop the dev cluster.
        return rc not in [0, -2]

    failed = False
    return_codes = await asyncio.gather(*all_coros)
    if any(failed_exit_code(rc) for rc in return_codes):
        print(f"Redpanda nodes exited with non-zero return codes: {return_codes}")
        failed = True

    async def stop_and_wait(
        name: str, process: Any, task: asyncio.Task[int] | None
    ) -> None:
        """Stop a process and wait for its task to complete, checking exit code."""
        nonlocal failed
        if task and process:
            print(f"Stopping {name}...")
            process.stop()
            ret_code = await task
            if failed_exit_code(ret_code):
                print(f"{name} exited with non-zero return code: {ret_code}")
                failed = True
            else:
                print(f"{name} stopped.")

    # Cleanup: if redpanda shuts down but we didn't request the shutdown
    # then let's go ahead and tear down other services too so we exit
    await stop_and_wait("minio", minio, minio_task)
    await stop_and_wait("prometheus", prometheus, prometheus_task)
    await stop_and_wait("grafana", grafana, grafana_task)

    if failed:
        exit(1)


asyncio.run(main())
