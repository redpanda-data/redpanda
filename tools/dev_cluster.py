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
# Start a 3 node cluster:
#
#   [jerry@winterland]$ dev_cluster.py -e vbuild/debug/clang/bin/redpanda
#
import argparse
import asyncio
import dataclasses
import json
import os
import pathlib
import shutil
import signal
import time
from typing import Optional

import aioboto3
import psutil
import yaml

BOOTSTRAP_YAML = ".bootstrap.yaml"


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
    data_directory: pathlib.Path
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
    unstable_beta_feature_cloud_topics_enabled: bool = False
    enable_developmental_unrecoverable_data_corrupting_features: int = int(time.time())


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
    config_dict: dict


class Minio:
    def __init__(self, binary, directory, rp_config):
        self.binary = binary
        self.directory = directory
        self.stopped = False
        self.rp_cfg = rp_config

    def stop(self):
        if not self.stopped:
            self.stopped = True
            self.process.send_signal(signal.SIGINT)

    async def run(self):
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
        args = " ".join(args)
        cmd = f"{args} 2>&1 | tee -i {log_path}"
        print(f"Running: {cmd}")
        self.process = await asyncio.create_subprocess_shell(
            cmd,
            env=env,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.STDOUT,
        )

        while True:
            line = await self.process.stdout.readline()
            if not line:
                break
            line = line.decode("utf8").rstrip()
            print(f"minio: {line}")

        await self.process.wait()


class Prometheus:
    def __init__(
        self,
        binary,
        directory,
        listen_address="127.0.0.1",
        port=3001,
        redpanda_admin_ports=[],
    ):
        self.binary = binary
        self.directory = directory
        self.stopped = False
        self.listen_address = listen_address
        self.port = port
        self.redpanda_admin_ports = redpanda_admin_ports

    def stop(self):
        if not self.stopped:
            self.stopped = True
            self.process.send_signal(signal.SIGINT)

    async def run(self):
        log_path = self.directory / "prometheus.log"
        data_dir = self.directory / "data"
        config_file = self.directory / "prometheus.yml"

        data_dir.mkdir(parents=True, exist_ok=True)

        # Create a basic Prometheus configuration
        config = {
            "global": {
                "scrape_interval": "5s",
                "evaluation_interval": "5s",
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
        args = " ".join(args)
        cmd = f"{args} 2>&1 | tee -i {log_path}"
        print(f"Running: {cmd}")
        print(f"Prometheus UI available at: http://{self.listen_address}:{self.port}")

        self.process = await asyncio.create_subprocess_shell(
            cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.STDOUT,
        )

        while True:
            line = await self.process.stdout.readline()
            if not line:
                break
            line = line.decode("utf8").rstrip()
            print(f"prometheus: {line}")

        await self.process.wait()


class Grafana:
    def __init__(
        self,
        binary,
        directory,
        port=3000,
        prometheus_url=None,
    ):
        self.binary = binary
        self.directory = directory
        self.stopped = False
        self.port = port
        self.prometheus_url = prometheus_url

    def stop(self):
        if not self.stopped:
            self.stopped = True
            self.process.send_signal(signal.SIGINT)

    async def run(self):
        log_path = self.directory / "grafana.log"
        grafana_home = self.directory / "home"
        grafana_home.mkdir(parents=True, exist_ok=True)

        # Copy grafana files (conf, public) into grafana_home
        grafana_binary = pathlib.Path(self.binary).resolve()
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
                    }
                ],
            }

            datasource_file = provisioning_dir / "prometheus.yml"
            with open(datasource_file, "w") as f:
                yaml.dump(datasource_config, f)
            print(f"Configured Prometheus datasource at {self.prometheus_url}")

        env = os.environ.copy()
        env["GF_SERVER_HTTP_ADDR"] = "0.0.0.0"
        env["GF_SERVER_HTTP_PORT"] = str(self.port)
        env["GF_SECURITY_ADMIN_PASSWORD"] = "admin"
        env["GF_SECURITY_ADMIN_USER"] = "admin"
        env["GF_AUTH_BASIC_ENABLED"] = "false"
        env["GF_AUTH_DISABLE_LOGIN_FORM"] = "true"
        env["GF_AUTH_ANONYMOUS_ENABLED"] = "true"
        env["GF_AUTH_ANONYMOUS_ORG_ROLE"] = "Admin"

        args = [str(grafana_binary), "server"]
        args = " ".join(args)
        cmd = f"{args} 2>&1 | tee -i {log_path}"
        print(f"Running: {cmd}")
        print(f"Grafana UI available on port {self.port}")

        self.process = await asyncio.create_subprocess_shell(
            cmd,
            env=env,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.STDOUT,
            cwd=grafana_home,
        )

        while True:
            line = await self.process.stdout.readline()
            if not line:
                break
            line = line.decode("utf8").rstrip()
            print(f"grafana: {line}")

        await self.process.wait()


class Redpanda:
    def __init__(self, binary, cores: int, node_meta: NodeMetadata, extra_args, env):
        self.binary = binary
        self.cores = cores
        self.node_meta = node_meta
        self.process = None
        self.extra_args = extra_args
        self.env = env

    def stop(self):
        print(f"node-{self.node_meta.index}: dev_cluster stop requested")
        self.process.send_signal(signal.SIGINT)

    async def run(self):
        log_path = (
            pathlib.Path(os.path.dirname(self.node_meta.config_path)) / "redpanda.log"
        )

        # If user did not override cores with extra args, apply it from our internal cores setting
        if not {"-c", "--smp"} & set(self.extra_args):
            # Caller is required to pass a finite core count
            assert self.cores > 0
            base_core = self.cores * self.node_meta.index

            cores_args = f"--cpuset {base_core}-{base_core + self.cores - 1}"
        else:
            cores_args = ""

        # If user did not specify memory, share 75% of memory equally between nodes
        if not {"-m", "--memory"} & set(self.extra_args):
            memory_total = psutil.virtual_memory().total
            memory_per_node = (3 * (memory_total // 4)) // self.node_meta.cluster_size
            memory_args = f"-m {memory_per_node // (1024 * 1024)}M"
        else:
            memory_args = ""

        extra_args = " ".join(f'"{a}"' for a in self.extra_args)

        self.process = await asyncio.create_subprocess_shell(
            f"{self.binary} --redpanda-cfg {self.node_meta.config_path} {cores_args} {memory_args} {extra_args} 2>&1 | tee -i {log_path}",
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.STDOUT,
            env=self.env,
        )

        while True:
            line = await self.process.stdout.readline()
            if not line:
                break
            line = line.decode("utf8").rstrip()
            print(f"node-{self.node_meta.index}: {line}")

        await self.process.wait()


async def run_command(cmd):
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


async def ensure_bucket_exists(cfg: dict):
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


async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-e",
        "--executable",
        type=pathlib.Path,
        help="path to redpanda executable",
        default="redpanda",
    )
    parser.add_argument(
        "--ubsan_suppression_file",
        type=pathlib.Path,
        help="path to ubsan_suppressions.txt",
    )
    parser.add_argument(
        "--lsan_suppression_file",
        type=pathlib.Path,
        help="path to lsan_suppressions.txt",
    )
    parser.add_argument("--nodes", type=int, help="number of nodes", default=3)
    parser.add_argument(
        "--cores", type=int, help="number of cores per node", default=None
    )
    parser.add_argument(
        "-d", "--directory", type=pathlib.Path, help="data directory", default=None
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
        type=pathlib.Path,
        help="path to minio executable",
        default="minio",
    )
    parser.add_argument(
        "--use-minio",
        action=argparse.BooleanOptionalAction,
        help="whether to spin up an instance of minio and use Redpanda configuration presets for it",
        default=True,
    )
    parser.add_argument(
        "--rpk", type=pathlib.Path, help="path to rpk executable", default=None
    )
    parser.add_argument(
        "--prometheus",
        type=pathlib.Path,
        help="path to prometheus executable",
        default=None,
    )
    parser.add_argument(
        "--use-prometheus",
        action=argparse.BooleanOptionalAction,
        help="whether to spin up an instance of prometheus",
        default=False,
    )
    parser.add_argument(
        "--grafana",
        type=pathlib.Path,
        help="path to grafana executable",
        default=None,
    )
    parser.add_argument(
        "--use-grafana",
        action=argparse.BooleanOptionalAction,
        help="whether to spin up an instance of grafana",
        default=False,
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
        args.directory = (
            pathlib.Path(os.environ.get("BUILD_WORKSPACE_DIRECTORY", ".")) / "data"
        )

    # Use the first 3 nodes as seed servers
    rpc_addresses = [
        NetworkAddress(args.listen_address, args.base_rpc_port + i)
        for i in range(args.nodes)
    ]

    def make_node_metadata(i, data_dir, config_path, rack):
        make_address = lambda p: NetworkAddress(args.listen_address, p + i)
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
            config_path=config_path,
            index=i,
            cluster_size=args.nodes,
            config_dict=dataclasses.asdict(node_conf),
        )

    def pathlib_path_representer(dumper, path):
        return dumper.represent_scalar("!Path", str(path))

    def get_config_dumper():
        d = yaml.SafeDumper
        d.add_representer(pathlib.PosixPath, pathlib_path_representer)
        return d

    def prepare_node(i, rack):
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
            yaml.dump(config_dict, f, indent=2, Dumper=get_config_dumper())

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
            prometheus_url=prometheus_url,
        )
        grafana_task = asyncio.create_task(grafana.run())

    cores = args.cores
    if cores is None:
        # Use 75% of cores for redpanda.  e.g. 3 node cluster on a 16 node system
        # gives each node 4 cores.
        cores = max((3 * (psutil.cpu_count(logical=False) // 4)) // args.nodes, 1)
    env = os.environ.copy()
    if "ASAN_OPTIONS" not in env:
        env["ASAN_OPTIONS"] = "disable_coredump=0:abort_on_error=1"
    if "UBSAN_OPTIONS" not in env:
        env["UBSAN_OPTIONS"] = "halt_on_error=1:abort_on_error=1:report_error_type=1"
        if args.ubsan_suppression_file:
            env["UBSAN_OPTIONS"] += f":suppressions={args.ubsan_suppression_file}"
    if args.lsan_suppression_file and "LSAN_OPTIONS" not in env:
        env["LSAN_OPTIONS"] = f"suppressions={args.lsan_suppression_file}"
    nodes = [Redpanda(args.executable, cores, m, extra_args, env) for m in node_metas]

    all_coros = [r.run() for r in nodes]

    def stop():
        for n in nodes:
            n.stop()
        if minio:
            minio.stop()
        if prometheus:
            prometheus.stop()
        if grafana:
            grafana.stop()

    asyncio.get_event_loop().add_signal_handler(signal.SIGINT, stop)

    await asyncio.gather(*all_coros)

    # Cleanup: if redpanda shuts down but we didn't request the shutdown
    # then let's go ahead and tear down other services too so we exit
    if minio_task and minio:
        minio.stop()
        await minio_task
    if prometheus_task and prometheus:
        prometheus.stop()
        await prometheus_task
    if grafana_task and grafana:
        grafana.stop()
        await grafana_task


asyncio.run(main())
