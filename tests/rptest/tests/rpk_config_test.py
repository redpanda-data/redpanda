# Copyright 2021 Redpanda Data, Inc.
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
from rptest.services.redpanda import RedpandaService, RESTART_LOG_ALLOW_LIST

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
    data_directory: /var/lib/redpanda/data
    node_id: 0
    seed_servers: []
    rpc_server:
        address: 0.0.0.0
        port: 33145
    kafka_api:
        - address: 0.0.0.0
          port: 9092
    admin:
        - address: 0.0.0.0
          port: 9644
    developer_mode: true
rpk:
    admin_api:
        addresses:
            - 127.0.0.1:9644
    coredump_dir: /var/lib/redpanda/coredump
    enable_memory_locking: false
    enable_usage_stats: false
    kafka_api:
        brokers:
            - 0.0.0.0:9092    
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

                expected_config = yaml.full_load(expected_out)
                actual_config = yaml.full_load(f.read())

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

        rpk.config_set(key, value, path=RedpandaService.NODE_CONFIG_FILE)

        with tempfile.TemporaryDirectory() as d:
            node.account.copy_from(RedpandaService.NODE_CONFIG_FILE, d)

            with open(os.path.join(d, config_file)) as f:
                actual_config = yaml.full_load(f.read())
                if f"{actual_config['redpanda']['admin'][0]['port']}" != value:
                    self.logger.error("Configs differ")
                    self.logger.error(f"Expected: {value}")
                    self.logger.error(
                        f"Actual: {yaml.dump(actual_config['redpanda']['admin'][0]['port'])}"
                    )
                assert f"{actual_config['redpanda']['admin'][0]['port']}" == value

    @cluster(num_nodes=3)
    def test_config_set_yaml(self):
        n = random.randint(1, len(self.redpanda.nodes))
        node = self.redpanda.get_node(n)
        rpk = RpkRemoteTool(self.redpanda, node)
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

        expected = '''                                                      
- host:
    address: 192.168.10.1
    port: 33145
- host:
    address: 192.168.10.2
    port: 33145
- host:
    address: 192.168.10.3
    port: 33145
'''

        rpk.config_set(key, value, format='yaml')

        with tempfile.TemporaryDirectory() as d:
            node.account.copy_from(RedpandaService.NODE_CONFIG_FILE, d)

            with open(os.path.join(d, 'redpanda.yaml')) as f:
                expected_config = yaml.full_load(expected)
                actual_config = yaml.full_load(f.read())
                if actual_config['redpanda']['seed_servers'] != expected_config:
                    self.logger.error("Configs differ")
                    self.logger.error(
                        f"Expected: {yaml.dump(expected_config)}")
                    self.logger.error(
                        f"Actual: {yaml.dump(actual_config['redpanda']['seed_servers'])}"
                    )
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

        expected_config = yaml.full_load('''
admin_api:
    addresses:
        - 127.0.0.1:9644
coredump_dir: /var/lib/redpanda/coredump
enable_memory_locking: false
enable_usage_stats: false  
overprovisioned: false
tune_aio_events: true
tune_ballast_file: false
tune_clocksource: false
tune_coredump: false
tune_cpu: true
tune_disk_irq: true
tune_disk_nomerges: false
tune_disk_scheduler: false
tune_disk_write_cache: false
tune_fstrim: false
tune_network: false
tune_swappiness: false
tune_transparent_hugepages: false
''')

        with tempfile.TemporaryDirectory() as d:
            node.account.copy_from(RedpandaService.NODE_CONFIG_FILE, d)

            with open(os.path.join(d, 'redpanda.yaml')) as f:
                actual_config = yaml.full_load(f.read())

                assert actual_config['rpk']['kafka_api'] is not None

                # Delete 'kafka_api' so they can be compared since the
                # brokers change depending on the container it's running
                del actual_config['rpk']['kafka_api']

                if actual_config['rpk'] != expected_config:
                    self.logger.error("Configs differ")
                    self.logger.error(
                        f"Expected: {yaml.dump(expected_config)}")
                    self.logger.error(
                        f"Actual: {yaml.dump(actual_config['rpk'])}")
                assert actual_config['rpk'] == expected_config

    @cluster(num_nodes=3, log_allow_list=RESTART_LOG_ALLOW_LIST)
    def test_config_change_then_restart_node(self):
        for node in self.redpanda.nodes:
            rpk = RpkRemoteTool(self.redpanda, node)
            key = 'redpanda.admin.port'
            value = '9641'  # The default is 9644, so we will change it

            rpk.config_set(key, value)

            self.redpanda.restart_nodes(node)

    @cluster(num_nodes=1)
    def test_config_change_mode_prod(self):
        """
        Verify that after running rpk redpanda mode prod, the 
        configuration values of the tuners change accordingly.
        """
        node = self.redpanda.nodes[0]
        rpk = RpkRemoteTool(self.redpanda, node)
        rpk.mode_set("prod")
        expected_config = '''
    tune_network: true
    tune_disk_scheduler: true
    tune_disk_nomerges: true
    tune_disk_write_cache: true
    tune_disk_irq: true
    tune_fstrim: false
    tune_cpu: true
    tune_aio_events: true
    tune_clocksource: true
    tune_swappiness: true
    tune_transparent_hugepages: false
    enable_memory_locking: false
    tune_coredump: false
    coredump_dir: /var/lib/redpanda/coredump
    tune_ballast_file: true
    overprovisioned: false
'''
        with tempfile.TemporaryDirectory() as d:
            node.account.copy_from(RedpandaService.NODE_CONFIG_FILE, d)

            with open(os.path.join(d, 'redpanda.yaml')) as f:
                actual_config = f.read()

                if expected_config not in actual_config:
                    self.logger.error("Configs differ")
                    self.logger.error(f"Substring: {expected_config}")
                    self.logger.error(f"Not in: {actual_config}")
                assert expected_config in actual_config
