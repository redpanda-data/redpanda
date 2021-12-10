# Copyright 2021 Vectorized, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

from rptest.services.cluster import cluster
from rptest.tests.redpanda_test import RedpandaTest
from rptest.clients.rpk_remote import RpkRemoteTool
from rptest.services.redpanda import RedpandaService

import yaml
import random
import tempfile
import os


class RpkConfigTest(RedpandaTest):
    def __init__(self, ctx):
        super(RpkConfigTest, self).__init__(test_context=ctx)
        self._ctx = ctx

    @cluster(num_nodes=3)
    def test_config_init(self):
        n = random.randint(0, len(self.redpanda.nodes))
        node = self.redpanda.get_node(n)
        rpk = RpkRemoteTool(self.redpanda, node)
        path = './redpanda-test.yaml'

        rpk.config_init(path)

        with tempfile.TemporaryDirectory() as d:
            node.account.copy_from(path, d)
            with open(os.path.join(d, path)) as f:
                expected_out = '''config_file: /root/redpanda-test.yaml
# node_uuid: (the uuid is random so we don't compare it)
pandaproxy: {}
redpanda:
  admin:
  - address: 0.0.0.0
    port: 9644
  data_directory: /var/lib/redpanda/data
  developer_mode: true
  kafka_api:
  - address: 0.0.0.0
    port: 9092
  node_id: 0
  rpc_server:
    address: 0.0.0.0
    port: 33145
  seed_servers: []
rpk:
  coredump_dir: /var/lib/redpanda/coredump
  enable_memory_locking: false
  enable_usage_stats: false
  overprovisioned: false
  tune_aio_events: false
  tune_ballast_file: false
  tune_clocksource: false
  tune_coredump: false
  tune_cpu: false
  tune_disk_irq: false
  tune_disk_nomerges: false
  tune_disk_scheduler: false
  tune_disk_write_cache: false
  tune_fstrim: false
  tune_network: false
  tune_swappiness: false
  tune_transparent_hugepages: false
schema_registry: {}
'''

                expected_config = yaml.load(expected_out)
                actual_config = yaml.load(f.read())

                assert actual_config['node_uuid'] is not None

                # Delete 'node_uuid' so they can be compared (it's random so
                # it's probably gonna be different each time)
                del actual_config['node_uuid']

                if actual_config != expected_config:
                    self.logger.error("Configs differ")
                    self.logger.error(
                        f"Expected: {yaml.dump(expected_config)}")
                    self.logger.error(f"Actual: {yaml.dump(actual_config)}")

                assert actual_config == expected_config

    @cluster(num_nodes=3)
    def test_config_set_single_number(self):
        n = random.randint(1, len(self.redpanda.nodes))
        node = self.redpanda.get_node(n)
        rpk = RpkRemoteTool(self.redpanda, node)
        config_file = 'redpanda.yaml'
        key = 'redpanda.admin.port'
        value = '9641'  # The default is 9644, so we will change it

        rpk.config_set(key, value, path=RedpandaService.CONFIG_FILE)

        with tempfile.TemporaryDirectory() as d:
            node.account.copy_from(RedpandaService.CONFIG_FILE, d)

            with open(os.path.join(d, config_file)) as f:
                actual_config = yaml.load(f.read())
                assert f"{actual_config['redpanda']['admin']['port']}" == value

    @cluster(num_nodes=3)
    def test_config_set_yaml(self):
        n = random.randint(1, len(self.redpanda.nodes))
        node = self.redpanda.get_node(n)
        rpk = RpkRemoteTool(self.redpanda, node)
        path = '/etc/redpanda/redpanda.yaml'
        key = 'redpanda.seed_servers'
        value = '''                                                      
- node_id: 1
  host:
    address: 192.168.10.1
    port: 33145
- node_id: 2
  host:
    address: 192.168.10.2
    port: 33145
- node_id: 3
  host:
    address: 192.168.10.3
    port: 33145
'''
        rpk.config_set(key, value, format='yaml')

        with tempfile.TemporaryDirectory() as d:
            node.account.copy_from(RedpandaService.CONFIG_FILE, d)

            with open(os.path.join(d, 'redpanda.yaml')) as f:
                expected_config = yaml.load(value)
                actual_config = yaml.load(f.read())
                assert actual_config['redpanda'][
                    'seed_servers'] == expected_config

    @cluster(num_nodes=3)
    def test_config_set_json(self):
        n = random.randint(1, len(self.redpanda.nodes))
        node = self.redpanda.get_node(n)
        rpk = RpkRemoteTool(self.redpanda, node)
        key = 'rpk'
        value = '{"tune_aio_events":true,"tune_cpu":true,"tune_disk_irq":true}'

        rpk.config_set(key, value, format='json')

        expected_config = yaml.load('''
coredump_dir: /var/lib/redpanda/coredump
enable_memory_locking: false
enable_usage_stats: false
tune_aio_events: true
tune_clocksource: false
tune_coredump: false
tune_cpu: true
tune_disk_irq: true
tune_disk_nomerges: false
tune_disk_scheduler: false
tune_fstrim: false
tune_network: false
tune_swappiness: false
''')

        with tempfile.TemporaryDirectory() as d:
            node.account.copy_from(RedpandaService.CONFIG_FILE, d)

            with open(os.path.join(d, 'redpanda.yaml')) as f:
                actual_config = yaml.load(f.read())

                assert actual_config['rpk'] == expected_config

    @cluster(num_nodes=3)
    def test_config_change_then_restart_node(self):
        for node in self.redpanda.nodes:
            rpk = RpkRemoteTool(self.redpanda, node)
            key = 'redpanda.admin.port'
            value = '9641'  # The default is 9644, so we will change it

            rpk.config_set(key, value)

            self.redpanda.restart_nodes(node)
