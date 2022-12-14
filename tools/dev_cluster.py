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
import pathlib
import yaml
import dataclasses
import argparse
import sys
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


class Redpanda:
    def __init__(self, binary, config):
        self.binary = binary
        self.config = config
        self.process = None

    async def run(self):
        log_path = pathlib.Path(os.path.dirname(self.config)) / "redpanda.log"
        self.process = await asyncio.create_subprocess_shell(
            f"{self.binary} --redpanda-cfg {self.config} 2>&1 | tee -i {log_path}",
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.STDOUT)

        while True:
            line = await self.process.stdout.readline()
            if not line:
                break
            line = line.decode("utf8").rstrip()
            print(f"{self.process.pid}: {line}")

        self.process.terminate()
        return await self.process.wait()


async def input_helper(configs):
    def dump_endpoints():
        for config, _ in configs:
            print("admin endpoint:",
                  f"http://localhost:{config.redpanda.admin.port}")

    while True:
        loop = asyncio.get_event_loop()
        content = await loop.run_in_executor(None, sys.stdin.readline)
        if "h" in content:
            dump_endpoints()


async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("-e",
                        "--executable",
                        type=pathlib.Path,
                        help="path to redpanda executable",
                        default="redpanda")
    parser.add_argument("--nodes", type=int, help="number of nodes", default=3)
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
    args = parser.parse_args()

    seed_servers = [
        NetworkAddress(args.listen_address, args.base_rpc_port + i)
        for i in range(args.nodes)
    ]

    def make_node_config(i, data_dir):
        make_address = lambda p: NetworkAddress(args.listen_address, p + i)
        rpc_address = seed_servers[i]
        redpanda = RedpandaConfig(data_directory=data_dir,
                                  rpc_server=rpc_address,
                                  advertised_rpc_api=rpc_address,
                                  kafka_api=make_address(args.base_kafka_port),
                                  admin=make_address(args.base_admin_port),
                                  seed_servers=seed_servers,
                                  empty_seed_starts_cluster=False)
        return NodeConfig(redpanda=redpanda)

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

        config = make_node_config(i, data_dir)
        with open(conf_file, "w") as f:
            yaml.dump(dataclasses.asdict(config),
                      f,
                      indent=2,
                      Dumper=get_config_dumper())

        # If there is a bootstrap file in pwd, propagate it to each node's
        # directory so that they'll load it on first start
        if os.path.exists(BOOTSTRAP_YAML):
            shutil.copyfile(BOOTSTRAP_YAML, node_dir / BOOTSTRAP_YAML)

        return config, conf_file

    configs = [prepare_node(i) for i in range(args.nodes)]
    nodes = [Redpanda(args.executable, c[1]) for c in configs]

    coros = [r.run() for r in nodes]
    coros.append(input_helper(configs))
    await asyncio.gather(*coros)


asyncio.run(main())
