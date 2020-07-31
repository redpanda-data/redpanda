import os
import time
import yaml

from ducktape.services.service import Service


class RedpandaService(Service):
    def __init__(self, context, num_nodes=3):
        super(RedpandaService, self).__init__(context, num_nodes=num_nodes)

        self.extra_config = {}
        extra_config_file = os.environ.get('RP_EXTRA_CONF', None)
        if extra_config_file:
            if not os.path.exists(extra_config_file):
                raise Exception("{} doesn't exist".format(extra_config_file))
            with open(extra_config_file, 'r') as f:
                self.extra_config = yaml.load(f)

    def start_node(self, node):
        rpk_config = self.get_config(node)
        self.logger.debug('Using config: {}'.format(rpk_config))

        node.account.create_file('/etc/redpanda/redpanda.yaml',
                                 yaml.dump(rpk_config))

        # wipe data directory
        node.account.ssh('rm -fr /var/lib/redpanda/*')

        rpk_flags = '--check=false --config=/etc/redpanda/redpanda.yaml'
        cmd = ('nohup rpk start --tune {} `</dev/null` > '
               '/var/lib/redpanda/stdouterr 2>&1 &').format(rpk_flags)

        node.account.ssh('sysctl -w fs.aio-max-nr=1048576')
        node.account.ssh(cmd)

        time.sleep(2)

        self.pid = node.account.ssh_output('cat /var/run/redpanda.pid')

        if not self.pid:
            out = node.account.ssh_output('cat /var/lib/redpanda/stdouterr')
            raise Exception('Unable to obtain PID of redpanda process. '
                            '\nstdouterr:\n{}' + out)

        if self.is_running(node):
            out = node.account.ssh_output('cat /var/lib/redpanda/stdouterr')
            raise Exception('Redpanda failed to start: {}.'.format(out))

    def is_running(self, node):
        ret = node.account.ssh('kill -0 {}'.format(self.pid), allow_fail=True)
        return ret != 0

    def get_config(self, node):
        node_idx = self.idx(node)
        kafka_port = 9092
        rpc_port = 33145

        seed_servers = []
        for n in self.nodes:
            seed_servers.append({
                'host': {
                    'address': str(n.account.hostname),
                    'port': rpc_port,
                },
                'node_id': self.idx(n)
            })

        cfg = {
            'pid_file': '/var/run/redpanda.pid',
            'redpanda': {
                'data_directory': "/var/lib/redpanda",
                'node_id': node_idx,
                'raft_heartbeat_interval': 2000,
                'rpc_server': {
                    'address': "0.0.0.0",
                    'port': rpc_port,
                },
                'advertised_rpc_api': {
                    'address': str(node.account.ssh_hostname),
                    'port': rpc_port,
                },
                'kafka_api': {
                    'address': "0.0.0.0",
                    'port': kafka_port,
                },
                'advertised_kafka_api': {
                    'address': str(node.account.ssh_hostname),
                    'port': kafka_port,
                },
            },
            'rpk': {
                'coredump_dir': '/var/lib/redpanda/coredump',
                'additional_start_flags': [
                    "--default-log-level=trace",
                ]
            }
        }
        # Node 1 will be the root node, so it has an empty seed_servers list.
        if node_idx > 1:
            cfg['seed_servers'] = seed_servers

        # extra config
        cfg.update(self.extra_config)

        return cfg

    def stop_node(self, node):
        if self.is_running(node):
            node.account.ssh('kill -15 {}'.format(self.pid))

    def bootstrap_servers(self,
                          protocol='PLAINTEXT',
                          validate=True,
                          offline_nodes=[]):
        """Comma-separated list of nodes in this cluster: HOST1:PORT1,...
        """
        return '{}:9092'.format(self.nodes[0].account.hostname)
