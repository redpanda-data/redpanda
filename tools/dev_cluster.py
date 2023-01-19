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
import asyncio
import psutil
import pathlib
import yaml
import dataclasses
import argparse
import signal
import os
import shutil

try:
    from rich import print
except ImportError:
    pass

BOOTSTRAP_YAML = ".bootstrap.yaml"


@dataclasses.dataclass
class NetworkAddress:
    address: str
    port: int


@dataclasses.dataclass
class RedpandaConfig:
    data_directory: pathlib.Path
    rpc_server: NetworkAddress
    advertised_rpc_api: NetworkAddress
    kafka_api: NetworkAddress
    admin: NetworkAddress
    seed_servers: list[NetworkAddress]
    empty_seed_starts_cluster: bool = False


@dataclasses.dataclass
class NodeConfig:
    redpanda: RedpandaConfig
    config_path: str

    # This is _not_ the node_id, just the index into our array of nodes
    index: int
    cluster_size: int


class Redpanda:
    def __init__(self, binary, cores: int, config: NodeConfig, extra_args):
        self.binary = binary
        self.cores = cores
        self.config = config
        self.process = None
        self.extra_args = extra_args

    def stop(self):
        print(f"{self.process.pid}: dev_cluster stop requested")
        self.process.send_signal(signal.SIGINT)

    async def run(self):
        log_path = pathlib.Path(os.path.dirname(
            self.config.config_path)) / "redpanda.log"

        # If user did not override cores with extra args, apply it from our internal cores setting
        if not {"-c", "--smp"} & set(self.extra_args):
            # Caller is required to pass a finite core count
            assert self.cores > 0
            base_core = self.cores * self.config.index

            cores_args = f"--cpuset {base_core}-{base_core + self.cores - 1}"
        else:
            cores_args = ""

        # If user did not specify memory, share 75% of memory equally between nodes
        if not {"-m", "--memory"} & set(self.extra_args):
            memory_total = psutil.virtual_memory().total
            memory_per_node = (3 *
                               (memory_total // 4)) // self.config.cluster_size
            memory_args = f"-m {memory_per_node // (1024 * 1024)}M"
        else:
            memory_args = ""

        extra_args = ' '.join(f"\"{a}\"" for a in self.extra_args)

        self.process = await asyncio.create_subprocess_shell(
            f"{self.binary} --redpanda-cfg {self.config.config_path} {cores_args} {memory_args} {extra_args} 2>&1 | tee -i {log_path}",
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.STDOUT)

        while True:
            line = await self.process.stdout.readline()
            if not line:
                break
            line = line.decode("utf8").rstrip()
            print(f"{self.process.pid}: {line}")

        await self.process.wait()


async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("-e",
                        "--executable",
                        type=pathlib.Path,
                        help="path to redpanda executable",
                        default="redpanda")
    parser.add_argument("--nodes", type=int, help="number of nodes", default=3)
    parser.add_argument("--cores",
                        type=int,
                        help="number of cores per node",
                        default=None)
    parser.add_argument("-d",
                        "--directory",
                        type=pathlib.Path,
                        help="data directory",
                        default="data")
    parser.add_argument("--base-rpc-port",
                        type=int,
                        help="rpc port",
                        default=33145)
    parser.add_argument("--base-kafka-port",
                        type=int,
                        help="kafka port",
                        default=9092)
    parser.add_argument("--base-admin-port",
                        type=int,
                        help="admin port",
                        default=9644)
    parser.add_argument("--listen-address",
                        type=str,
                        help="listening address",
                        default="0.0.0.0")
    args, extra_args = parser.parse_known_args()

    if extra_args and extra_args[0] == "--":
        extra_args = extra_args[1:]
    elif extra_args:
        # Re-do with strict parse: this will surface unknown argument errors
        args = parser.parse_args()

    seed_servers = [
        NetworkAddress(args.listen_address, args.base_rpc_port + i)
        for i in range(args.nodes)
    ]

    def make_node_config(i, data_dir, config_path):
        make_address = lambda p: NetworkAddress(args.listen_address, p + i)
        rpc_address = seed_servers[i]
        redpanda = RedpandaConfig(data_directory=data_dir,
                                  rpc_server=rpc_address,
                                  advertised_rpc_api=rpc_address,
                                  kafka_api=make_address(args.base_kafka_port),
                                  admin=make_address(args.base_admin_port),
                                  seed_servers=seed_servers,
                                  empty_seed_starts_cluster=False)
        return NodeConfig(redpanda=redpanda,
                          index=i,
                          config_path=config_path,
                          cluster_size=args.nodes)

    def pathlib_path_representer(dumper, path):
        return dumper.represent_scalar("!Path", str(path))

    def get_config_dumper():
        d = yaml.SafeDumper
        d.add_representer(pathlib.PosixPath, pathlib_path_representer)
        return d

    def prepare_node(i):
        node_dir = args.directory / f"node{i}"
        data_dir = node_dir / "data"
        conf_file = node_dir / "config.yaml"

        node_dir.mkdir(parents=True, exist_ok=True)
        data_dir.mkdir(parents=True, exist_ok=True)

        config = make_node_config(i, data_dir, conf_file)
        with open(conf_file, "w") as f:
            yaml.dump(dataclasses.asdict(config),
                      f,
                      indent=2,
                      Dumper=get_config_dumper())

        # If there is a bootstrap file in pwd, propagate it to each node's
        # directory so that they'll load it on first start
        if os.path.exists(BOOTSTRAP_YAML):
            shutil.copyfile(BOOTSTRAP_YAML, node_dir / BOOTSTRAP_YAML)

        return config

    configs = [prepare_node(i) for i in range(args.nodes)]

    cores = args.cores
    if cores is None:
        # Use 75% of cores for redpanda.  e.g. 3 node cluster on a 16 node system
        # gives each node 4 cores.
        cores = max((3 * (psutil.cpu_count(logical=False) // 4)) // args.nodes,
                    1)
    nodes = [Redpanda(args.executable, cores, c, extra_args) for c in configs]

    coros = [r.run() for r in nodes]

    def stop():
        for n in nodes:
            n.stop()

    asyncio.get_event_loop().add_signal_handler(signal.SIGINT, stop)

    await asyncio.gather(*coros)


asyncio.run(main())
