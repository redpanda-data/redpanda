# Copyright 2023 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

from collections import defaultdict
from rptest.services.redpanda import RedpandaService

import psutil


class NetemSpec():
    def __init__(self, delay_ms, jitter_ms=None, loss=None):
        self.delay_ms = delay_ms
        self.jitter_ms = 0.1 * self.delay_ms if jitter_ms is None else jitter_ms
        self.loss = loss


class NodeQdisc():
    def __init__(self, node, device_to_use, number_of_prio_bands):
        self.node = node
        self.dev = device_to_use
        self.default_bands = 2 + number_of_prio_bands
        self.flow = 3

    def initialize(self):
        # create PRIO queue with n+1 bands where n is number of classes to add
        self._tc_qdisc_add(
            ['root', 'handle', '1:', 'prio', 'bands',
             str(self.default_bands)])
        # add SFQ for all not delayed traffic
        self._tc_qdisc_add(['parent', '1:1', 'handle', '10:', 'sfq'])

    def add(self, target_addresses, spec: NetemSpec):
        queue = [
            'parent', f'1:{self.flow}', 'handle', f'{self.flow}0:', 'netem',
            'delay', f'{spec.delay_ms}ms', f'{spec.jitter_ms}ms', '20.00'
        ]
        if spec.loss:
            queue += ['loss', str(spec.loss)]
        self._tc_qdisc_add(queue_spec=queue)

        for a in target_addresses:
            self._tc_execute([
                'filter', 'add', 'dev', self.dev, 'protocol', 'ip', 'parent',
                '1:0', 'prio', '1', 'u32', 'match', 'ip', 'dst', a, 'flowid',
                f'1:{self.flow}'
            ])

        self.flow += 1

    def _tc_qdisc_add(self, queue_spec):
        return self._tc_execute(['qdisc', 'add', 'dev', self.dev] + queue_spec)

    def list_queues(self):
        return self._tc_execute(['qdisc', 'list', 'dev', self.dev])

    def _tc_execute(self, cmd):
        whole_cmd = ['tc'] + cmd
        return self.node.account.ssh_output(' '.join(whole_cmd))

    def __enter__(self):
        self.initialize()
        return self

    def __exit__(self, type, value, traceback):
        self.remove_all()

    def remove_all(self):
        self._tc_execute(['qdisc', 'del', 'dev', self.dev, 'root'])


class TopologyNode():
    def __init__(self, region=None, rack=None):
        self.region = region
        self.rack = rack


class TopologyConnectionSpec():
    def __init__(self, source: TopologyNode, target: TopologyNode,
                 spec: NetemSpec):
        self.source = source
        self.target = target
        self.spec = spec


class Rack():
    def __init__(self, name):
        self.name = name
        self.nodes = []

    def add_node(self, node):
        self.nodes.append(node)

    def __str__(self):
        return f"rack: {self.name}, nodes: [{','.join([n.account.hostname for n in self.nodes])}]"


class Region():
    def __init__(self, name):
        self.name = name
        self.racks = {}

    def add_rack(self, rack: Rack):
        assert rack.name not in self.racks, f"rack {rack.name} already exists in {self.name}"
        self.racks[rack.name] = rack

    def __str__(self):
        return f"region: {self.name}, racks: {[str(r) for _, r in self.racks.items()]}"


class ClusterTopology():
    UNASSIGNED_REGION = "__ducktape_unassigned"

    IGNORED_IF_PREFIXES = ["veth", "docker", "lo", "br"]

    def __init__(self):
        self.regions = {}
        self.network_configuration = []
        self.node_qdisc = []
        self.network_device = ClusterTopology._get_if_in_use()

    @staticmethod
    def _get_if_in_use():
        """Picks the interface for tc traffic shaping."""
        available_ifs = psutil.net_if_addrs().keys()

        # Not all environments (container, VM etc) have the same interface naming
        # convention (eg: eth0 vs ensX in EC2). Here we filter out all known patterns
        # like virtual/bridge/loopback interfaces that can ignored and pick the first
        # available interface that doesn't match the ignore filter. This may break
        # as we run in newer environments that do not conform to these naming rules
        # in which case we need to update the prefix filters.
        def ignored(i):
            return any(
                [i.startswith(p) for p in ClusterTopology.IGNORED_IF_PREFIXES])

        available = [i for i in available_ifs if not ignored(i)]
        assert available, f"No suitable interface found in {available_ifs}"
        return next(iter(available))

    def _unassigned_region(self):
        return Region(ClusterTopology.UNASSIGNED_REGION)

    def add_region(self, region: Region):
        assert region not in self.regions, f"region {region} already exists in cluster"
        self.regions[region.name] = region

    def add_rack(self, rack: Rack):
        if ClusterTopology.UNASSIGNED_REGION not in self.regions:
            self.add_region(Region(ClusterTopology.UNASSIGNED_REGION))

        self.regions[ClusterTopology.UNASSIGNED_REGION].add_rack(rack)

    def add_rack(self, region_name, rack: Rack):
        assert region_name in self.regions, f"can not add rack to region {region_name} that does not exists"
        self.regions[region_name].add_rack(rack)

    def get_region(self, region_name: str):
        return self.regions[region_name]

    def add_node(self, region_name, rack_name, node):
        assert region_name in self.regions, "can not add node to region that does not exists"
        region: Region = self.regions[region_name]

        assert rack_name in region.racks, f"can not add node to the rack {rack_name} that does not exists in {region_name} region"
        region.racks[rack_name].add_node(node)

    def nodes_in(self, region=None, rack=None):
        region = ClusterTopology.UNASSIGNED_REGION if region is None else region

        if region in self.regions:
            rg = self.regions[region]
            if rack is None:
                return sum([r.nodes for r in rg.racks.values()], [])
            elif rack in rg.racks:
                return rg.racks[rack].nodes

        return []

    def _ip_address(self, node):
        res = node.account.ssh_output(f'hostname -i')
        return res.strip().decode('utf-8')

    def add_connection_spec(self, spec: TopologyConnectionSpec):
        self.network_configuration.append(spec)

    def start_with_topology(self, redpanda: RedpandaService, **kwargs):
        node_overrides = {}
        for n in redpanda.nodes:
            node_tn = self.placement_of(n)
            override = {}

            #TODO: change this part as soon as Redpanda will support hierarchical topology
            override['rack'] = f"{node_tn.region}.{node_tn.rack}"
            node_overrides[n] = override

        redpanda.start(node_config_overrides=node_overrides, **kwargs)

    def enable_traffic_shaping(self):
        per_node_configurations = defaultdict(list)

        for nc in self.network_configuration:
            source_nodes = self.nodes_in(region=nc.source.region,
                                         rack=nc.source.rack)
            target_nodes = self.nodes_in(region=nc.target.region,
                                         rack=nc.target.rack)
            target_ips = [self._ip_address(n) for n in target_nodes]

            for src_node in source_nodes:
                if src_node not in per_node_configurations:
                    per_node_configurations[src_node].append(
                        (nc.spec, target_ips))

        for node, specs in per_node_configurations.items():
            qdisc = NodeQdisc(node, self.network_device, len(specs))
            qdisc.initialize()
            for [netem_spec, target_ips] in specs:
                qdisc.add(target_addresses=target_ips, spec=netem_spec)

            self.node_qdisc.append(qdisc)

    def disable_traffic_shaping(self):
        for n in self.node_qdisc:
            n.remove_all()

    def placement_of(self, node):
        def region_name(region):
            return region.name if region.name != ClusterTopology.UNASSIGNED_REGION else None

        for _, region in self.regions.items():
            for _, rack in region.racks.items():
                if node in rack.nodes:
                    return TopologyNode(region=region_name(region),
                                        rack=rack.name)
        return None

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        self.disable_traffic_shaping()

    def __str__(self):
        return f"{[str(r) for _,r in self.regions.items()]}"
