import time
import yaml

from ducktape.services.service import Service


class RedpandaService(Service):
    CONFIG = '/etc/redpanda/redpanda.yml'

    def __init__(self, context, num_nodes=3):
        super(RedpandaService, self).__init__(context, num_nodes=num_nodes)

    def start_node(self, node):
        rp_config = self.get_config(node)

        self.logger.debug('Config file contents:')
        self.logger.debug(rp_config)

        node.account.create_file(RedpandaService.CONFIG, yaml.dump(rp_config))

        cmd = (
            'nohup redpanda --redpanda-cfg {} `</dev/null` > nohup.out 2>&1 &'
        ).format(RedpandaService.CONFIG)

        node.account.ssh(cmd)

        time.sleep(2)

        self.pid = node.account.ssh_output('pgrep redpanda')

        if not self.pid:
            raise Exception('Unable to obtain PID of redpanda process.')

        # TODO: ensure that service was started (check redpanda process)

    def get_config(self, node):
        node_idx = self.idx(node)
        kafka_port = 9092
        rpc_port = 33145

        cfg = {
            'redpanda': {
                'data_directory':
                "/var/lib/redpanda",
                'node_id':
                node_idx,
                'raft_heartbeat_interval':
                2000,
                'rpc_server': {
                    'address': "0.0.0.0",
                    'port': rpc_port,
                },
                'advertised_rpc_api': {
                    'address': str(node.account.hostname),
                    'port': rpc_port,
                },
                'kafka_api': {
                    'address': "0.0.0.0",
                    'port': kafka_port,
                },
                'advertised_kafka_api': {
                    'address': str(node.account.hostname),
                    'port': kafka_port,
                },
                # Raft configuration
                'seed_servers': [{
                    'host': {
                        'address': str(self.get_node(1).account.hostname),
                        'port': rpc_port,
                    },
                    'node_id': 1
                }]
            }
        }

        return cfg

    def stop_node(self, node):
        node.account.ssh('kill {}'.format(self.pid))

    def bootstrap_servers(self,
                          protocol='PLAINTEXT',
                          validate=True,
                          offline_nodes=[]):
        """Comma-separated list of nodes in this cluster: HOST1:PORT1,...
        """
        return '{}:9092'.format(self.nodes[0].account.hostname)
