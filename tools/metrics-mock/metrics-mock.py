#!/usr/bin/env python3

# generate life-like redpanda metrics to stdout or on a mock /metrics endpoint
# over http

from abc import ABC, abstractmethod
from enum import Enum, auto
from functools import partial
from http.server import BaseHTTPRequestHandler, HTTPServer
import os
import random
import string
import sys
from timeit import timeit
from typing import Any, BinaryIO, cast
from dataclasses import dataclass
import itertools
import argparse
from math import ceil
from typing import Iterable


class Endpoint(Enum):
    PRIVATE = auto()
    PUBLIC = auto()


@dataclass
class Label:
    name: str
    value: str

    def __str__(self):
        return f'{self.name}="{self.value}"'


@dataclass
class Metric:
    name: str
    labels: list[Label]
    value: float

    @property
    def label_names(self):
        return [l.name for l in self.labels]


@dataclass
class Partition():
    topic: str
    part_id: int
    shard_id: int


@dataclass
class GroupOffset():
    group: str
    topic: str
    part_id: int


@dataclass
class Context:
    shard_count: int
    partitions: list[Partition]
    groups: list[GroupOffset]
    ep: Endpoint

    # labels are part of the context since they can change
    # depending on the endpoint
    def prefix(self, name: str):
        return 'redpanda_' + name if self.ep == Endpoint.PUBLIC else name


class PLabel(Label):
    """Automatically prefixed label"""
    def __init__(self, context: Context, name: str, value: str):
        super().__init__(context.prefix(name), value)


class LGen(ABC):
    @abstractmethod
    def gen(self, context: Context) -> list[list[Label]]:
        ...


class TopicGen(LGen):
    def __init__(self, has_partition: bool = False, has_shard: bool = True):
        self.has_partition = has_partition
        self.has_shard = has_shard

    def gen(self, context: Context) -> list[list[Label]]:
        all_labels: list[list[Label]] = []
        seen_topics: set[str] = set()
        for p in context.partitions:
            labels: list[Label] = []
            if not self.has_partition and p.topic in seen_topics:
                continue
            seen_topics.add(p.topic)
            labels.append(PLabel(context, 'topic', p.topic))
            labels.append(PLabel(context, 'namespace', "kafka"))
            if self.has_shard:
                labels.append(Label("shard", str(p.shard_id)))
            if self.has_partition:
                labels.append(PLabel(context, 'partition', str(p.part_id)))
            all_labels.append(labels)
        return all_labels


class PartitionGen(TopicGen):
    def __init__(self, has_shard: bool):
        super().__init__(has_partition=True, has_shard=has_shard)


class GroupOffsetGen(LGen):
    def gen(self, context: Context) -> list[list[Label]]:
        all_labels: list[list[Label]] = []
        for g in context.groups:
            labels: list[Label] = []
            labels.append(PLabel(context, 'group', g.group))
            labels.append(PLabel(context, 'partition', str(g.part_id)))
            labels.append(PLabel(context, 'topic', g.topic))
            # shard is basically random, 0 is as good as any
            labels.append(Label('shard', '0'))
            all_labels.append(labels)
        return all_labels


def to_string(name: str, labels_nested: Iterable[list[Label]]):
    labels: list[Label] = []
    for llist in labels_nested:
        for label in llist:
            labels.append(label)
    return name + '{' + ','.join(sorted(map(str, labels))) + '} '


def expand_label_lists(context: Context,
                       labels: list[LGen]) -> list[tuple[list[Label]]]:
    expanded = [l.gen(context) for l in labels]
    all: Any = itertools.product(*expanded)
    return list(all)


@dataclass
class MetricDef:
    name: str
    help: str
    typ: str
    label_gens: list[LGen]

    @property
    def header(self) -> str:
        name = self.name
        if self.typ == 'histogram':
            if name.endswith('_sum'):
                name = name.removesuffix('_sum')
            else:
                # only output help on the first metric (_sum) as the rest don't get help
                return ''
        return f'# HELP {name} {self.help}\n# TYPE {name} {self.typ}\n'

    def expand(self, context: Context):
        expanded = expand_label_lists(context, self.label_gens)
        stringed = [to_string(self.name, e) for e in expanded]
        stringed[0] = self.header + stringed[0]
        return stringed


class ShardGen(LGen):
    """Generate a shard="n" label for all N shards"""
    def gen(self, context: Context) -> list[list[Label]]:
        return [[Label("shard", str(id))] for id in range(context.shard_count)]


class IOShardGen(LGen):
    """Generate a [shard="n", ioshard="n"] label set for all N shards"""
    def gen(self, context: Context) -> list[list[Label]]:
        return [[Label("shard", str(id)),
                 Label("ioshard", str(id))]
                for id in range(context.shard_count)]


class ListGen(LGen):
    """Generate labels from an explicit list passed at construction"""
    def __init__(self, name: str, values: list[str]):
        self.name = name
        self.values = values

    def gen(self, context: Context) -> list[list[Label]]:
        return [[Label(self.name, v)] for v in self.values]


class MetricsEmulator:
    def __init__(self, args: Any, metric_defs: list[MetricDef], ep: Endpoint):

        shards = cast(int, args.shards)
        partitions = cast(int, args.partitions)
        topics = cast(int, args.topics)
        rf = cast(int, args.rf)
        nodes = cast(int, args.nodes)
        node_id = cast(int, args.node_id)
        name_len = cast(int, args.name_length)
        groups_per_topic = cast(int, args.groups_per_topic)

        # rf for __consumer_offsets
        co_rf = 3 if nodes >= 3 else 1

        assert node_id < nodes, f'node-id {node_id} out of range [0, {nodes-1}]'
        assert rf > 0
        assert topics > 0 or partitions == 0
        assert name_len >= 12, 'name should be 11 chars or more'

        # Now we "generate" every ntp in the cluster and figure out which
        # are assigned to this node, which is used by partition/topic-based
        # generators. We also need to handleconsumer groups, which are used
        # only by a couple of metrics but are potentially very large so
        # important. Every topic will have N groups assocaited with it, and
        # we check for each topic if any of these groups "landed" on this
        # node (__consumer_offsets has a fixed replication factor of 3), and
        # if so populate the GroupDef struct as well with *all* the partitions
        # from the current topic.
        partition_list: list[Partition] = []
        group_list: list[GroupOffset] = []
        assigned_node = 0
        assigned_shard = 0
        total_partitions = 0
        current_group = 0
        for topic_id in range(topics):
            prefix = f'topic-{topic_id:05}-'
            rem = name_len - len(prefix)
            assert rem >= 0
            name = prefix + ''.join(random.choices(string.ascii_letters,
                                                   k=rem))
            # calculate how many partitions this topic should have based on the total
            # partition target for all topics so far (first term) minus assigned
            # partitions from previous term: this song and dance is to fix the "rounding"
            # problem if you specify say 50 partitions and 40 topics, you need some
            # partitions to have 1 partition and some to have 2: this accomplishes that
            part_count = ceil((topic_id + 1) / topics * partitions -
                              total_partitions)
            total_partitions += part_count

            # handle groups, the * rf comes from the replicas for __consumer_groups
            # topic: assuming rf=3 for that topic, each partition in __co will have
            # a replica on 3 nodes, so 3 nodes will report all offsets for that group
            for _ in range(groups_per_topic * co_rf):
                if current_group % nodes == node_id:
                    group_name = f'group-{current_group}-{name}'
                    for part_id in range(part_count):
                        group_list.append(
                            GroupOffset(group_name, name, part_id))
                current_group += 1

            for part_id in [p for p in range(part_count) for _ in range(rf)]:
                if assigned_node == node_id:
                    partition_list.append(
                        Partition(name, part_id, assigned_shard))
                    assigned_shard = (assigned_shard + 1) % shards
                assigned_node = (assigned_node +
                                 1) % nodes  # round robin around the nodes

        context = Context(shard_count=shards,
                          partitions=partition_list,
                          groups=group_list,
                          ep=ep)

        self.prefixes = [
            one_prefix.encode() for m in metric_defs
            for one_prefix in m.expand(context)
        ]

    def print(self, file: Any = sys.stdout):
        self.print_binary(file=file.buffer)

    def print_binary(self, file: BinaryIO):
        for p in self.prefixes:
            buf = bytearray(p)
            v = str(random.randrange(15000)) + '\n'
            buf.extend(v.encode())
            file.write(buf)


class MetricsHandler(BaseHTTPRequestHandler):

    print_once = True

    wbufsize = 128 * 1024

    def __init__(self, cmd_args: Any, im: MetricsEmulator, pm: MetricsEmulator,
                 *args: Any, **kwargs: Any):
        self.im = im
        self.pm = pm
        if MetricsHandler.print_once:
            print(
                f'Mock metrics server up on port {cmd_args.port}\n'
                f'Internal metrics: {len(self.im.prefixes)}\n'
                f'Public metrics  : {len(self.pm.prefixes)}\n',
                file=sys.stderr)
            MetricsHandler.print_once = False
        super().__init__(*args, **kwargs)

    def do_HEAD(self):
        self.send_response(200)
        self.send_header('Content-type', 'text/plain')
        self.end_headers()

    def do_GET(self):
        if self.path == '/metrics':
            self.do_HEAD()
            self.im.print_binary(self.wfile)
        elif self.path == '/public_metrics':
            self.do_HEAD()
            self.pm.print_binary(self.wfile)
        else:
            self.send_error(404, explain=f'bad path: {self.path}')


def main(internal_metrics: list[MetricDef], public_metrics: list[MetricDef]):

    # deterministic output
    random.seed(12345)

    parser = argparse.ArgumentParser()

    parser.add_argument(
        'command',
        help='The number of shards per node',
        type=str,
        metavar='COMMAND: print print-public host',
        choices=['print', 'print-public', 'host', 'time-print'])

    parser.add_argument('--shards',
                        help='The number of shards per node',
                        type=int,
                        default=1)

    parser.add_argument('--partitions',
                        help='The total number of rf=3 partitions',
                        type=int,
                        default=1000)

    parser.add_argument('--rf',
                        help='The replication factor for the topics',
                        type=int,
                        default=3)

    parser.add_argument('--topics',
                        help='The number of topics to spread the partitions '
                        ' across (partitions are assigned evenly to topics +/-'
                        ' 1 partition due to rounding)',
                        type=int,
                        default=10)

    parser.add_argument('--groups-per-topic',
                        help='The number of consumer groups associated'
                        ' with each topic.'
                        ' The assumption is each group consumes only'
                        ' from 1 topic, and '
                        'the number is set with this argument.'
                        '0 is valid.',
                        type=int,
                        default=2)

    parser.add_argument('--nodes',
                        help='The total number of nodes in the cluster',
                        type=int,
                        default=3)

    parser.add_argument('--node-id',
                        help='The node this mock should pretend to be',
                        type=int,
                        default=0)

    parser.add_argument('--name-length',
                        help='The number of characters in the topic name',
                        type=int,
                        default=100)

    parser.add_argument('--port',
                        help='The listening port for the metrics server',
                        type=int,
                        default=9644)

    args = parser.parse_args()

    def make_mi(ep: Endpoint):
        return MetricsEmulator(
            args,
            internal_metrics if ep == Endpoint.PRIVATE else public_metrics,
            ep=ep)

    if args.command == 'print':
        make_mi(Endpoint.PRIVATE).print()
    elif args.command == 'print-public':
        make_mi(Endpoint.PUBLIC).print()
    elif args.command == 'host':
        server_address = ('', args.port)
        handler = partial(MetricsHandler, args, make_mi(Endpoint.PRIVATE),
                          make_mi(Endpoint.PUBLIC))
        HTTPServer(server_address, handler).serve_forever()
    elif args.command == 'time-print':
        em = make_mi(Endpoint.PRIVATE)
        print(
            f'Time to generate {len(em.prefixes)} metrics in seconds:',
            timeit(lambda: em.print(file=open(os.devnull, "wt")), number=10) /
            10)
    else:
        assert False


def m(
    gens: list[LGen],
    name: str,
    help: str,
    typ: str,
):
    return MetricDef(name, help, typ, list(gens))


# yapf: disable

METRICS = [
    m([ShardGen()], 'vectorized_alien_receive_batch_queue_length', 'Current receive batch queue length', 'gauge'),
    m([ShardGen()], 'vectorized_alien_total_received_messages', 'Total number of received messages', 'counter'),
    m([ShardGen()], 'vectorized_alien_total_sent_messages', 'Total number of sent messages', 'counter'),
    m([ShardGen(), ListGen('revision', ['fd1019bc7ee99e5821c645fe54c1c835c2188e69']), ListGen('version', ['v23.2.21'])], 'vectorized_application_build', 'Redpanda build information', 'gauge'),
    m([ShardGen()], 'vectorized_application_uptime', 'Redpanda uptime in milliseconds', 'gauge'),
    m([ShardGen()], 'vectorized_archival_upload_backlog_controller_backlog_size', 'controller backlog', 'gauge'),
    m([ShardGen()], 'vectorized_archival_upload_backlog_controller_error', 'current controller error, i.e difference between set point and backlog size', 'gauge'),
    m([ShardGen()], 'vectorized_archival_upload_backlog_controller_shares', 'controller output, i.e. number of shares', 'gauge'),
    m([ShardGen(), ListGen('endpoint', ['s3.us-west-2.amazonaws.com']), ListGen('region', ['us-west-2'])], 'vectorized_cloud_client_active_downloads', 'Number of active GET requests at the moment', 'gauge'),
    m([ShardGen(), ListGen('endpoint', ['s3.us-west-2.amazonaws.com']), ListGen('region', ['us-west-2'])], 'vectorized_cloud_client_active_requests', 'Number of active HTTP requests at the moment (includes PUT and GET)', 'gauge'),
    m([ShardGen(), ListGen('endpoint', ['s3.us-west-2.amazonaws.com']), ListGen('region', ['us-west-2'])], 'vectorized_cloud_client_active_uploads', 'Number of active PUT requests at the moment', 'gauge'),
    m([ShardGen(), ListGen('endpoint', ['s3.us-west-2.amazonaws.com']), ListGen('region', ['us-west-2'])], 'vectorized_cloud_client_all_requests', 'Number of completed HTTP requests (includes PUT and GET)', 'counter'),
    m([ShardGen(), ListGen('endpoint', ['s3.us-west-2.amazonaws.com']), ListGen('region', ['us-west-2'])], 'vectorized_cloud_client_client_pool_utilization', 'Utilization of the cloud storage pool(0 - unused, 100 - fully utilized)', 'gauge'),
    m([ShardGen(), ListGen('endpoint', ['s3.us-west-2.amazonaws.com']), ListGen('region', ['us-west-2'])], 'vectorized_cloud_client_lease_duration_sum', 'Lease duration histogram', 'histogram'),
    m([ShardGen(), ListGen('endpoint', ['s3.us-west-2.amazonaws.com']), ListGen('region', ['us-west-2'])], 'vectorized_cloud_client_lease_duration_count', 'Lease duration histogram', 'histogram'),
    m([ShardGen(), ListGen('endpoint', ['s3.us-west-2.amazonaws.com']), ListGen('le', ['0.000255','0.000511','0.001023','0.002047','0.004095','0.008191','0.016383','0.032767','0.065535','0.131071','0.262143','0.524287','1.048575','2.097151','4.194303','8.388607','16.777215','33.554431','+Inf']), ListGen('region', ['us-west-2'])], 'vectorized_cloud_client_lease_duration_bucket', 'Lease duration histogram', 'histogram'),
    m([ShardGen(), ListGen('endpoint', ['s3.us-west-2.amazonaws.com']), ListGen('region', ['us-west-2'])], 'vectorized_cloud_client_num_borrows', 'Number of time current shard had to borrow a cloud storage client from another shard', 'counter'),
    m([ShardGen(), ListGen('endpoint', ['s3.us-west-2.amazonaws.com']), ListGen('region', ['us-west-2'])], 'vectorized_cloud_client_num_nosuchkey', 'Total number of NoSuchKey errors received from cloud storage provider', 'counter'),
    m([ShardGen(), ListGen('endpoint', ['s3.us-west-2.amazonaws.com']), ListGen('region', ['us-west-2'])], 'vectorized_cloud_client_num_rpc_errors', 'Total number of REST API errors received from cloud storage provider', 'counter'),
    m([ShardGen(), ListGen('endpoint', ['s3.us-west-2.amazonaws.com']), ListGen('region', ['us-west-2'])], 'vectorized_cloud_client_num_slowdowns', 'Total number of SlowDown errors received from cloud storage provider', 'counter'),
    m([ShardGen(), ListGen('endpoint', ['s3.us-west-2.amazonaws.com']), ListGen('region', ['us-west-2'])], 'vectorized_cloud_client_num_transport_errors', 'Total number of transport errors (TCP and TLS)', 'counter'),
    m([ShardGen(), ListGen('endpoint', ['s3.us-west-2.amazonaws.com']), ListGen('region', ['us-west-2'])], 'vectorized_cloud_client_total_downloads', 'Number of completed GET requests', 'counter'),
    m([ShardGen(), ListGen('endpoint', ['s3.us-west-2.amazonaws.com']), ListGen('region', ['us-west-2'])], 'vectorized_cloud_client_total_inbound_bytes', 'Total number of bytes received from cloud storage', 'counter'),
    m([ShardGen(), ListGen('endpoint', ['s3.us-west-2.amazonaws.com']), ListGen('region', ['us-west-2'])], 'vectorized_cloud_client_total_outbound_bytes', 'Total number of bytes sent to cloud storage', 'counter'),
    m([ShardGen(), ListGen('endpoint', ['s3.us-west-2.amazonaws.com']), ListGen('region', ['us-west-2'])], 'vectorized_cloud_client_total_uploads', 'Number of completed PUT requests', 'counter'),
    m([ShardGen()], 'vectorized_cloud_roles__auth_refresh_fetch_errors', 'Total errors while fetching', 'counter'),
    m([ShardGen()], 'vectorized_cloud_roles__auth_refresh_successful_fetches', 'Total successful credential fetches', 'counter'),
    m([ShardGen()], 'vectorized_cloud_storage_bytes_received', 'Number of bytes received from cloud storage', 'counter'),
    m([ShardGen()], 'vectorized_cloud_storage_bytes_sent', 'Number of bytes sent to cloud storage', 'counter'),
    m([ShardGen()], 'vectorized_cloud_storage_cache_cached_gets', 'Total number of get requests that are already in cache.', 'counter'),
    m([ShardGen()], 'vectorized_cloud_storage_cache_files', 'Current number of files in cache.', 'gauge'),
    m([ShardGen()], 'vectorized_cloud_storage_cache_gets', 'Total number of cache get requests.', 'counter'),
    m([ShardGen()], 'vectorized_cloud_storage_cache_in_progress_files', 'Current number of files that are being put to cache.', 'gauge'),
    m([ShardGen()], 'vectorized_cloud_storage_cache_puts', 'Total number of files put into cache.', 'counter'),
    m([ShardGen()], 'vectorized_cloud_storage_cache_size_bytes', 'Current cache size in bytes.', 'gauge'),
    m([ShardGen()], 'vectorized_cloud_storage_client_acquisition_latency_sum', 'Client acquisition latency histogram', 'histogram'),
    m([ShardGen()], 'vectorized_cloud_storage_client_acquisition_latency_count', 'Client acquisition latency histogram', 'histogram'),
    m([ShardGen(), ListGen('le', ['0.000255','0.000511','0.001023','0.002047','0.004095','0.008191','0.016383','0.032767','0.065535','0.131071','0.262143','0.524287','1.048575','2.097151','4.194303','8.388607','16.777215','33.554431','+Inf'])], 'vectorized_cloud_storage_client_acquisition_latency_bucket', 'Client acquisition latency histogram', 'histogram'),
    m([ShardGen()], 'vectorized_cloud_storage_cluster_metadata_manifest_downloads', 'Number of partition manifest downloads', 'counter'),
    m([ShardGen()], 'vectorized_cloud_storage_cluster_metadata_manifest_uploads', 'Number of partition manifest uploads', 'counter'),
    m([ShardGen()], 'vectorized_cloud_storage_download_backoff', 'Number of times backoff  was applied during log-segment downloads', 'counter'),
    m([ShardGen()], 'vectorized_cloud_storage_failed_downloads', 'Number of failed log-segment downloads', 'counter'),
    m([ShardGen()], 'vectorized_cloud_storage_failed_index_downloads', 'Number of failed segment index downloads', 'counter'),
    m([ShardGen()], 'vectorized_cloud_storage_failed_index_uploads', 'Number of failed segment index uploads', 'counter'),
    m([ShardGen()], 'vectorized_cloud_storage_failed_manifest_downloads', 'Number of failed manifest downloads', 'counter'),
    m([ShardGen()], 'vectorized_cloud_storage_failed_manifest_uploads', 'Number of failed manifest uploads', 'counter'),
    m([ShardGen()], 'vectorized_cloud_storage_failed_uploads', 'Number of failed log-segment uploads', 'counter'),
    m([ShardGen()], 'vectorized_cloud_storage_index_downloads', 'Number of segment indices downloaded', 'counter'),
    m([ShardGen()], 'vectorized_cloud_storage_index_uploads', 'Number of segment indices uploaded', 'counter'),
    m([ShardGen()], 'vectorized_cloud_storage_manifest_download_backoff', 'Number of times backoff was applied during manifest download', 'counter'),
    m([ShardGen()], 'vectorized_cloud_storage_manifest_upload_backoff', 'Number of times backoff was applied during manifest upload', 'counter'),
    m([TopicGen(has_shard=False)], 'vectorized_cloud_storage_partition_chunk_size', 'Size of chunk downloaded from cloud storage', 'gauge'),
    m([ShardGen()], 'vectorized_cloud_storage_partition_manifest_downloads', 'Number of partition manifest downloads', 'counter'),
    m([ShardGen()], 'vectorized_cloud_storage_partition_manifest_uploads', 'Number of partition manifest (re)uploads', 'counter'),
    m([TopicGen(has_shard=False)], 'vectorized_cloud_storage_partition_read_bytes', 'Total bytes read from remote partition', 'counter'),
    m([TopicGen(has_shard=False)], 'vectorized_cloud_storage_partition_read_records', 'Total number of records read from remote partition', 'counter'),
    m([], 'vectorized_cloud_storage_read_path_chunk_hydration_latency_sum', 'Chunk hydration latency histogram', 'histogram'),
    m([], 'vectorized_cloud_storage_read_path_chunk_hydration_latency_count', 'Chunk hydration latency histogram', 'histogram'),
    m([ListGen('le', ['0.000255','0.000511','0.001023','0.002047','0.004095','0.008191','0.016383','0.032767','0.065535','0.131071','0.262143','0.524287','1.048575','2.097151','4.194303','8.388607','16.777215','33.554431','+Inf'])], 'vectorized_cloud_storage_read_path_chunk_hydration_latency_bucket', 'Chunk hydration latency histogram', 'histogram'),
    m([], 'vectorized_cloud_storage_read_path_chunks_hydrated', 'Total number of hydrated chunks (some may have been evicted from the cache)', 'counter'),
    m([], 'vectorized_cloud_storage_read_path_materialized_segments', 'Current number of materialized remote segments', 'gauge'),
    m([], 'vectorized_cloud_storage_read_path_readers', 'Current number of remote partition readers', 'gauge'),
    m([], 'vectorized_cloud_storage_read_path_segment_readers', 'Current number of remote segment readers', 'gauge'),
    m([], 'vectorized_cloud_storage_read_path_spillover_manifest_bytes', 'Total amount of memory used by spillover manifests', 'gauge'),
    m([], 'vectorized_cloud_storage_read_path_spillover_manifest_hydrated', 'Number of times spillover manifests were saved to the cache', 'counter'),
    m([], 'vectorized_cloud_storage_read_path_spillover_manifest_instances', 'Total number of spillover manifests stored in memory', 'gauge'),
    m([], 'vectorized_cloud_storage_read_path_spillover_manifest_latency_sum', 'Spillover manifest materialization latency histogram', 'histogram'),
    m([], 'vectorized_cloud_storage_read_path_spillover_manifest_latency_count', 'Spillover manifest materialization latency histogram', 'histogram'),
    m([ListGen('le', ['0.000255','0.000511','0.001023','0.002047','0.004095','0.008191','0.016383','0.032767','0.065535','0.131071','0.262143','0.524287','1.048575','2.097151','4.194303','8.388607','16.777215','33.554431','+Inf'])], 'vectorized_cloud_storage_read_path_spillover_manifest_latency_bucket', 'Spillover manifest materialization latency histogram', 'histogram'),
    m([], 'vectorized_cloud_storage_read_path_spillover_manifest_materialized', 'Number of times spillover manifests were loaded from the cache', 'counter'),
    m([ShardGen()], 'vectorized_cloud_storage_segment_download_latency_sum', 'Segment download latency histogram', 'histogram'),
    m([ShardGen()], 'vectorized_cloud_storage_segment_download_latency_count', 'Segment download latency histogram', 'histogram'),
    m([ShardGen(), ListGen('le', ['0.000255','0.000511','0.001023','0.002047','0.004095','0.008191','0.016383','0.032767','0.065535','0.131071','0.262143','0.524287','1.048575','2.097151','4.194303','8.388607','16.777215','33.554431','+Inf'])], 'vectorized_cloud_storage_segment_download_latency_bucket', 'Segment download latency histogram', 'histogram'),
    m([ShardGen()], 'vectorized_cloud_storage_spillover_manifest_downloads', 'Number of spillover manifest downloads', 'counter'),
    m([ShardGen()], 'vectorized_cloud_storage_spillover_manifest_uploads', 'Number of spillover manifest (re)uploads', 'counter'),
    m([ShardGen()], 'vectorized_cloud_storage_successful_downloads', 'Number of completed log-segment downloads', 'counter'),
    m([ShardGen()], 'vectorized_cloud_storage_successful_uploads', 'Number of completed log-segment uploads', 'counter'),
    m([ShardGen()], 'vectorized_cloud_storage_topic_manifest_downloads', 'Number of topic manifest downloads', 'counter'),
    m([ShardGen()], 'vectorized_cloud_storage_topic_manifest_uploads', 'Number of topic manifest uploads', 'counter'),
    m([ShardGen()], 'vectorized_cloud_storage_upload_backoff', 'Number of times backoff was applied during log-segment uploads', 'counter'),
    m([ShardGen()], 'vectorized_cluster_controller_pending_partition_operations', 'Number of partitions with ongoing/requested operations', 'gauge'),
    m([ShardGen()], 'vectorized_cluster_members_backend_queued_node_operations', 'Number of queued node operations', 'gauge'),
    m([PartitionGen(has_shard=False)], 'vectorized_cluster_partition_bytes_fetched_total', 'Total number of bytes fetched (not all might be returned to the client)', 'counter'),
    m([PartitionGen(has_shard=False)], 'vectorized_cluster_partition_bytes_produced_total', 'Total number of bytes produced', 'counter'),
    m([PartitionGen(has_shard=False)], 'vectorized_cluster_partition_cloud_storage_segments_metadata_bytes', 'Current number of bytes consumed by remote segments managed for this partition', 'counter'),
    m([PartitionGen(has_shard=False)], 'vectorized_cluster_partition_committed_offset', 'Partition commited offset. i.e. safely persisted on majority of replicas', 'gauge'),
    m([PartitionGen(has_shard=False)], 'vectorized_cluster_partition_end_offset', 'Last offset stored by current partition on this node', 'gauge'),
    m([PartitionGen(has_shard=False)], 'vectorized_cluster_partition_high_watermark', 'Partion high watermark i.e. highest consumable offset', 'gauge'),
    m([PartitionGen(has_shard=False)], 'vectorized_cluster_partition_last_stable_offset', 'Last stable offset', 'gauge'),
    m([PartitionGen(has_shard=False)], 'vectorized_cluster_partition_leader', 'Flag indicating if this partition instance is a leader', 'gauge'),
    m([PartitionGen(has_shard=False)], 'vectorized_cluster_partition_leader_id', 'Id of current partition leader', 'gauge'),
    m([], 'vectorized_cluster_partition_moving_from_node', 'Amount of partitions that are moving from node', 'gauge'),
    m([], 'vectorized_cluster_partition_moving_to_node', 'Amount of partitions that are moving to node', 'gauge'),
    m([], 'vectorized_cluster_partition_node_cancelling_movements', 'Amount of cancelling partition movements for node', 'gauge'),
    m([], 'vectorized_cluster_partition_num_with_broken_rack_constraint', "Number of partitions that don't satisfy the rack awareness constraint", 'gauge'),
    m([PartitionGen(has_shard=False)], 'vectorized_cluster_partition_records_fetched', 'Total number of records fetched', 'counter'),
    m([PartitionGen(has_shard=False)], 'vectorized_cluster_partition_records_produced', 'Total number of records produced', 'counter'),
    m([PartitionGen(has_shard=False)], 'vectorized_cluster_partition_start_offset', 'start offset', 'gauge'),
    m([PartitionGen(has_shard=False)], 'vectorized_cluster_partition_under_replicated_replicas', 'Number of under replicated replicas', 'gauge'),
    m([ListGen('fetch_result', ['non-empty','empty']), ListGen('latency_metric', ['microseconds'])], 'vectorized_fetch_stats_plan_and_execute_latency_us_sum', 'Latency of fetch planning and excution', 'histogram'),
    m([ListGen('fetch_result', ['non-empty','empty']), ListGen('latency_metric', ['microseconds'])], 'vectorized_fetch_stats_plan_and_execute_latency_us_count', 'Latency of fetch planning and excution', 'histogram'),
    m([ListGen('fetch_result', ['non-empty','empty']), ListGen('latency_metric', ['microseconds']), ListGen('le', ['7.000000','15.000000','31.000000','63.000000','127.000000','255.000000','511.000000','1023.000000','2047.000000','4095.000000','8191.000000','16383.000000','32767.000000','65535.000000','131071.000000','262143.000000','524287.000000','1048575.000000','2097151.000000','4194303.000000','8388607.000000','16777215.000000','33554431.000000','67108863.000000','134217727.000000','268435455.000000','+Inf'])], 'vectorized_fetch_stats_plan_and_execute_latency_us_bucket', 'Latency of fetch planning and excution', 'histogram'),
    m([ShardGen(), ListGen('service', ['admin','pandaproxy','schema_registry'])], 'vectorized_httpd_connections_current', 'The current number of open  connections', 'gauge'),
    m([ShardGen(), ListGen('service', ['admin','pandaproxy','schema_registry'])], 'vectorized_httpd_connections_total', 'The total number of connections opened', 'counter'),
    m([ShardGen(), ListGen('service', ['admin','pandaproxy','schema_registry'])], 'vectorized_httpd_read_errors', 'The total number of errors while reading http requests', 'counter'),
    m([ShardGen(), ListGen('service', ['admin','pandaproxy','schema_registry'])], 'vectorized_httpd_reply_errors', 'The total number of errors while replying to http', 'counter'),
    m([ShardGen(), ListGen('service', ['admin','pandaproxy','schema_registry'])], 'vectorized_httpd_requests_served', 'The total number of http requests served', 'counter'),
    m([], 'vectorized_internal_rpc_active_connections', 'internal_rpc: Currently active connections', 'gauge'),
    m([], 'vectorized_internal_rpc_connection_close_errors', 'internal_rpc: Number of errors when shutting down the connection', 'counter'),
    m([], 'vectorized_internal_rpc_connections_rejected', 'internal_rpc: Number of connections rejected for hitting connection limits', 'counter'),
    m([], 'vectorized_internal_rpc_connections_wait_rate', 'internal_rpc: Number of connections are blocked by connection rate', 'counter'),
    m([], 'vectorized_internal_rpc_connects', 'internal_rpc: Number of accepted connections', 'counter'),
    m([ShardGen()], 'vectorized_internal_rpc_consumed_mem_bytes', 'internal_rpc: Memory consumed by request processing', 'counter'),
    m([], 'vectorized_internal_rpc_corrupted_headers', 'internal_rpc: Number of requests with corrupted headers', 'counter'),
    m([ShardGen()], 'vectorized_internal_rpc_dispatch_handler_latency_sum', 'internal_rpc: Latency ', 'histogram'),
    m([ShardGen()], 'vectorized_internal_rpc_dispatch_handler_latency_count', 'internal_rpc: Latency ', 'histogram'),
    m([ShardGen(), ListGen('le', ['7.000000','15.000000','31.000000','63.000000','127.000000','255.000000','511.000000','1023.000000','2047.000000','4095.000000','8191.000000','16383.000000','32767.000000','65535.000000','131071.000000','262143.000000','524287.000000','1048575.000000','2097151.000000','4194303.000000','8388607.000000','16777215.000000','33554431.000000','67108863.000000','134217727.000000','268435455.000000','+Inf'])], 'vectorized_internal_rpc_dispatch_handler_latency_bucket', 'internal_rpc: Latency ', 'histogram'),
    m([ListGen('service', ['ephemeral_credential','partition_balancer_rpc','node_status_rpc','self_test_rpc','cluster_bootstrap','controller','topic_recovery_status_rpc','id_allocator','raftgen','metadata_dissemination_rpc','tx_gateway'])], 'vectorized_internal_rpc_latency_sum', 'Internal RPC service latency', 'histogram'),
    m([ListGen('service', ['ephemeral_credential','partition_balancer_rpc','node_status_rpc','self_test_rpc','cluster_bootstrap','controller','topic_recovery_status_rpc','id_allocator','raftgen','metadata_dissemination_rpc','tx_gateway'])], 'vectorized_internal_rpc_latency_count', 'Internal RPC service latency', 'histogram'),
    m([ListGen('le', ['7.000000','15.000000','31.000000','63.000000','127.000000','255.000000','511.000000','1023.000000','2047.000000','4095.000000','8191.000000','16383.000000','32767.000000','65535.000000','131071.000000','262143.000000','524287.000000','1048575.000000','2097151.000000','4194303.000000','8388607.000000','16777215.000000','33554431.000000','67108863.000000','134217727.000000','268435455.000000','+Inf']), ListGen('service', ['ephemeral_credential','partition_balancer_rpc','node_status_rpc','self_test_rpc','cluster_bootstrap','controller','topic_recovery_status_rpc','id_allocator','raftgen','metadata_dissemination_rpc','tx_gateway'])], 'vectorized_internal_rpc_latency_bucket', 'Internal RPC service latency', 'histogram'),
    m([ShardGen()], 'vectorized_internal_rpc_max_service_mem_bytes', 'internal_rpc: Maximum memory allowed for RPC', 'counter'),
    m([], 'vectorized_internal_rpc_method_not_found_errors', 'internal_rpc: Number of requests with not available RPC method', 'counter'),
    m([], 'vectorized_internal_rpc_received_bytes', 'internal_rpc: Number of bytes received from the clients in valid requests', 'counter'),
    m([], 'vectorized_internal_rpc_requests_blocked_memory', 'internal_rpc: Number of requests blocked in memory backpressure', 'counter'),
    m([], 'vectorized_internal_rpc_requests_completed', 'internal_rpc: Number of successful requests', 'counter'),
    m([], 'vectorized_internal_rpc_requests_pending', 'internal_rpc: Number of requests pending in the queue', 'gauge'),
    m([], 'vectorized_internal_rpc_sent_bytes', 'internal_rpc: Number of bytes sent to clients', 'counter'),
    m([], 'vectorized_internal_rpc_service_errors', 'internal_rpc: Number of service errors', 'counter'),
    m([IOShardGen(), ListGen('class', ['archival','compaction','default','raft','kafka_read']), ListGen('mountpoint', ['none']), ListGen('stream', ['rw'])], 'vectorized_io_queue_adjusted_consumption', 'Consumed disk capacity units adjusted for class shares and idling preemption', 'counter'),
    m([IOShardGen(), ListGen('class', ['archival','compaction','default','raft','kafka_read']), ListGen('mountpoint', ['none']), ListGen('stream', ['rw'])], 'vectorized_io_queue_consumption', 'Accumulated disk capacity units consumed by this class; an increment per-second rate indicates full utilization', 'counter'),
    m([IOShardGen(), ListGen('class', ['archival','compaction','default','raft','kafka_read']), ListGen('mountpoint', ['none'])], 'vectorized_io_queue_delay', 'random delay time in the queue', 'gauge'),
    m([IOShardGen(), ListGen('class', ['archival','compaction','default','raft','kafka_read']), ListGen('mountpoint', ['none'])], 'vectorized_io_queue_disk_queue_length', 'Number of requests in the disk', 'gauge'),
    m([IOShardGen(), ListGen('class', ['archival','compaction','default','raft','kafka_read']), ListGen('mountpoint', ['none'])], 'vectorized_io_queue_queue_length', 'Number of requests in the queue', 'gauge'),
    m([IOShardGen(), ListGen('class', ['archival','compaction','default','raft','kafka_read']), ListGen('mountpoint', ['none'])], 'vectorized_io_queue_shares', 'current amount of shares', 'gauge'),
    m([IOShardGen(), ListGen('class', ['archival','compaction','default','raft','kafka_read']), ListGen('mountpoint', ['none'])], 'vectorized_io_queue_starvation_time_sec', 'Total time spent starving for disk', 'counter'),
    m([IOShardGen(), ListGen('class', ['archival','compaction','default','raft','kafka_read']), ListGen('mountpoint', ['none'])], 'vectorized_io_queue_total_bytes', 'Total bytes passed in the queue', 'counter'),
    m([IOShardGen(), ListGen('class', ['archival','compaction','default','raft','kafka_read']), ListGen('mountpoint', ['none'])], 'vectorized_io_queue_total_delay_sec', 'Total time spent in the queue', 'counter'),
    m([IOShardGen(), ListGen('class', ['archival','compaction','default','raft','kafka_read']), ListGen('mountpoint', ['none'])], 'vectorized_io_queue_total_exec_sec', 'Total time spent in disk', 'counter'),
    m([IOShardGen(), ListGen('class', ['archival','compaction','default','raft','kafka_read']), ListGen('mountpoint', ['none'])], 'vectorized_io_queue_total_operations', 'Total operations passed in the queue', 'counter'),
    m([IOShardGen(), ListGen('class', ['archival','compaction','default','raft','kafka_read']), ListGen('mountpoint', ['none'])], 'vectorized_io_queue_total_read_bytes', 'Total read bytes passed in the queue', 'counter'),
    m([IOShardGen(), ListGen('class', ['archival','compaction','default','raft','kafka_read']), ListGen('mountpoint', ['none'])], 'vectorized_io_queue_total_read_ops', 'Total read operations passed in the queue', 'counter'),
    m([IOShardGen(), ListGen('class', ['archival','compaction','default','raft','kafka_read']), ListGen('mountpoint', ['none'])], 'vectorized_io_queue_total_split_bytes', 'Total number of bytes split', 'counter'),
    m([IOShardGen(), ListGen('class', ['archival','compaction','default','raft','kafka_read']), ListGen('mountpoint', ['none'])], 'vectorized_io_queue_total_split_ops', 'Total number of requests split', 'counter'),
    m([IOShardGen(), ListGen('class', ['archival','compaction','default','raft','kafka_read']), ListGen('mountpoint', ['none'])], 'vectorized_io_queue_total_write_bytes', 'Total write bytes passed in the queue', 'counter'),
    m([IOShardGen(), ListGen('class', ['archival','compaction','default','raft','kafka_read']), ListGen('mountpoint', ['none'])], 'vectorized_io_queue_total_write_ops', 'Total write operations passed in the queue', 'counter'),
    m([ShardGen()], 'vectorized_kafka_fetch_sessions_cache_mem_usage_bytes', 'Fetch sessions cache memory usage in bytes', 'gauge'),
    m([ShardGen()], 'vectorized_kafka_fetch_sessions_cache_sessions_count', 'Total number of fetch sessions', 'gauge'),
    m([GroupOffsetGen()], 'vectorized_kafka_group_offset', 'Group topic partition offset', 'gauge'),
    m([ListGen('handler', ['sasl_handshake','sasl_authenticate','offset_fetch','offset_delete','list_transactions','list_groups','metadata','join_group','list_partition_reassignments','init_producer_id','add_offsets_to_txn','incremental_alter_configs','fetch','offset_commit','create_topics','describe_log_dirs','describe_transactions','describe_producers','heartbeat','describe_groups','list_offsets','describe_configs','unknown_handler','sync_group','delete_records','end_txn','describe_acls','delete_groups','delete_topics','create_partitions','offset_for_leader_epoch','find_coordinator','api_versions','produce','create_acls','alter_partition_reassignments','txn_offset_commit','delete_acls','leave_group','alter_configs','add_partitions_to_txn'])], 'vectorized_kafka_handler_latency_microseconds_sum', 'Latency histogram of kafka requests', 'histogram'),
    m([ListGen('handler', ['sasl_handshake','sasl_authenticate','offset_fetch','offset_delete','list_transactions','list_groups','metadata','join_group','list_partition_reassignments','init_producer_id','add_offsets_to_txn','incremental_alter_configs','fetch','offset_commit','create_topics','describe_log_dirs','describe_transactions','describe_producers','heartbeat','describe_groups','list_offsets','describe_configs','unknown_handler','sync_group','delete_records','end_txn','describe_acls','delete_groups','delete_topics','create_partitions','offset_for_leader_epoch','find_coordinator','api_versions','produce','create_acls','alter_partition_reassignments','txn_offset_commit','delete_acls','leave_group','alter_configs','add_partitions_to_txn'])], 'vectorized_kafka_handler_latency_microseconds_count', 'Latency histogram of kafka requests', 'histogram'),
    m([ListGen('handler', ['sasl_handshake','sasl_authenticate','offset_fetch','offset_delete','list_transactions','list_groups','metadata','join_group','list_partition_reassignments','init_producer_id','add_offsets_to_txn','incremental_alter_configs','fetch','offset_commit','create_topics','describe_log_dirs','describe_transactions','describe_producers','heartbeat','describe_groups','list_offsets','describe_configs','unknown_handler','sync_group','delete_records','end_txn','describe_acls','delete_groups','delete_topics','create_partitions','offset_for_leader_epoch','find_coordinator','api_versions','produce','create_acls','alter_partition_reassignments','txn_offset_commit','delete_acls','leave_group','alter_configs','add_partitions_to_txn']), ListGen('le', ['7.000000','15.000000','31.000000','63.000000','127.000000','255.000000','511.000000','1023.000000','2047.000000','4095.000000','8191.000000','16383.000000','32767.000000','65535.000000','131071.000000','262143.000000','524287.000000','1048575.000000','2097151.000000','4194303.000000','8388607.000000','16777215.000000','33554431.000000','67108863.000000','134217727.000000','268435455.000000','+Inf'])], 'vectorized_kafka_handler_latency_microseconds_bucket', 'Latency histogram of kafka requests', 'histogram'),
    m([ListGen('handler', ['sasl_handshake','sasl_authenticate','offset_fetch','offset_delete','list_transactions','list_groups','metadata','join_group','list_partition_reassignments','init_producer_id','add_offsets_to_txn','incremental_alter_configs','fetch','offset_commit','create_topics','describe_log_dirs','describe_transactions','describe_producers','heartbeat','describe_groups','list_offsets','describe_configs','unknown_handler','sync_group','delete_records','end_txn','describe_acls','delete_groups','delete_topics','create_partitions','offset_for_leader_epoch','find_coordinator','api_versions','produce','create_acls','alter_partition_reassignments','txn_offset_commit','delete_acls','leave_group','alter_configs','add_partitions_to_txn'])], 'vectorized_kafka_handler_received_bytes_total', 'Number of bytes received from kafka requests', 'counter'),
    m([ListGen('handler', ['sasl_handshake','sasl_authenticate','offset_fetch','offset_delete','list_transactions','list_groups','metadata','join_group','list_partition_reassignments','init_producer_id','add_offsets_to_txn','incremental_alter_configs','fetch','offset_commit','create_topics','describe_log_dirs','describe_transactions','describe_producers','heartbeat','describe_groups','list_offsets','describe_configs','unknown_handler','sync_group','delete_records','end_txn','describe_acls','delete_groups','delete_topics','create_partitions','offset_for_leader_epoch','find_coordinator','api_versions','produce','create_acls','alter_partition_reassignments','txn_offset_commit','delete_acls','leave_group','alter_configs','add_partitions_to_txn'])], 'vectorized_kafka_handler_requests_completed_total', 'Number of kafka requests completed', 'counter'),
    m([ListGen('handler', ['sasl_handshake','sasl_authenticate','offset_fetch','offset_delete','list_transactions','list_groups','metadata','join_group','list_partition_reassignments','init_producer_id','add_offsets_to_txn','incremental_alter_configs','fetch','offset_commit','create_topics','describe_log_dirs','describe_transactions','describe_producers','heartbeat','describe_groups','list_offsets','describe_configs','unknown_handler','sync_group','delete_records','end_txn','describe_acls','delete_groups','delete_topics','create_partitions','offset_for_leader_epoch','find_coordinator','api_versions','produce','create_acls','alter_partition_reassignments','txn_offset_commit','delete_acls','leave_group','alter_configs','add_partitions_to_txn'])], 'vectorized_kafka_handler_requests_errored_total', 'Number of kafka requests errored', 'counter'),
    m([ListGen('handler', ['sasl_handshake','sasl_authenticate','offset_fetch','offset_delete','list_transactions','list_groups','metadata','join_group','list_partition_reassignments','init_producer_id','add_offsets_to_txn','incremental_alter_configs','fetch','offset_commit','create_topics','describe_log_dirs','describe_transactions','describe_producers','heartbeat','describe_groups','list_offsets','describe_configs','unknown_handler','sync_group','delete_records','end_txn','describe_acls','delete_groups','delete_topics','create_partitions','offset_for_leader_epoch','find_coordinator','api_versions','produce','create_acls','alter_partition_reassignments','txn_offset_commit','delete_acls','leave_group','alter_configs','add_partitions_to_txn'])], 'vectorized_kafka_handler_requests_in_progress_total', 'A running total of kafka requests in progress', 'counter'),
    m([ListGen('handler', ['sasl_handshake','sasl_authenticate','offset_fetch','offset_delete','list_transactions','list_groups','metadata','join_group','list_partition_reassignments','init_producer_id','add_offsets_to_txn','incremental_alter_configs','fetch','offset_commit','create_topics','describe_log_dirs','describe_transactions','describe_producers','heartbeat','describe_groups','list_offsets','describe_configs','unknown_handler','sync_group','delete_records','end_txn','describe_acls','delete_groups','delete_topics','create_partitions','offset_for_leader_epoch','find_coordinator','api_versions','produce','create_acls','alter_partition_reassignments','txn_offset_commit','delete_acls','leave_group','alter_configs','add_partitions_to_txn'])], 'vectorized_kafka_handler_sent_bytes_total', 'Number of bytes sent in kafka replies', 'counter'),
    m([ListGen('latency_metric', ['microseconds'])], 'vectorized_kafka_latency_fetch_latency_us_sum', 'Fetch Latency', 'histogram'),
    m([ListGen('latency_metric', ['microseconds'])], 'vectorized_kafka_latency_fetch_latency_us_count', 'Fetch Latency', 'histogram'),
    m([ListGen('latency_metric', ['microseconds']), ListGen('le', ['7.000000','15.000000','31.000000','63.000000','127.000000','255.000000','511.000000','1023.000000','2047.000000','4095.000000','8191.000000','16383.000000','32767.000000','65535.000000','131071.000000','262143.000000','524287.000000','1048575.000000','2097151.000000','4194303.000000','8388607.000000','16777215.000000','33554431.000000','67108863.000000','134217727.000000','268435455.000000','+Inf'])], 'vectorized_kafka_latency_fetch_latency_us_bucket', 'Fetch Latency', 'histogram'),
    m([ListGen('latency_metric', ['microseconds'])], 'vectorized_kafka_latency_produce_latency_us_sum', 'Produce Latency', 'histogram'),
    m([ListGen('latency_metric', ['microseconds'])], 'vectorized_kafka_latency_produce_latency_us_count', 'Produce Latency', 'histogram'),
    m([ListGen('latency_metric', ['microseconds']), ListGen('le', ['7.000000','15.000000','31.000000','63.000000','127.000000','255.000000','511.000000','1023.000000','2047.000000','4095.000000','8191.000000','16383.000000','32767.000000','65535.000000','131071.000000','262143.000000','524287.000000','1048575.000000','2097151.000000','4194303.000000','8388607.000000','16777215.000000','33554431.000000','67108863.000000','134217727.000000','268435455.000000','+Inf'])], 'vectorized_kafka_latency_produce_latency_us_bucket', 'Produce Latency', 'histogram'),
    m([ShardGen()], 'vectorized_kafka_quotas_balancer_runs', 'Number of times throughput quota balancer has been run', 'counter'),
    m([ListGen('direction', ['ingress','egress'])], 'vectorized_kafka_quotas_quota_effective', 'Currently effective quota, in bytes/s', 'counter'),
    m([], 'vectorized_kafka_quotas_traffic_intake', 'Amount of Kafka traffic received from the clients that is taken into processing, in bytes', 'counter'),
    m([], 'vectorized_kafka_rpc_active_connections', 'kafka_rpc: Currently active connections', 'gauge'),
    m([], 'vectorized_kafka_rpc_connection_close_errors', 'kafka_rpc: Number of errors when shutting down the connection', 'counter'),
    m([], 'vectorized_kafka_rpc_connections_rejected', 'kafka_rpc: Number of connections rejected for hitting connection limits', 'counter'),
    m([], 'vectorized_kafka_rpc_connections_wait_rate', 'kafka_rpc: Number of connections are blocked by connection rate', 'counter'),
    m([], 'vectorized_kafka_rpc_connects', 'kafka_rpc: Number of accepted connections', 'counter'),
    m([ShardGen()], 'vectorized_kafka_rpc_consumed_mem_bytes', 'kafka_rpc: Memory consumed by request processing', 'counter'),
    m([], 'vectorized_kafka_rpc_corrupted_headers', 'kafka_rpc: Number of requests with corrupted headers', 'counter'),
    m([ShardGen()], 'vectorized_kafka_rpc_dispatch_handler_latency_sum', 'kafka_rpc: Latency ', 'histogram'),
    m([ShardGen()], 'vectorized_kafka_rpc_dispatch_handler_latency_count', 'kafka_rpc: Latency ', 'histogram'),
    m([ShardGen(), ListGen('le', ['7.000000','15.000000','31.000000','63.000000','127.000000','255.000000','511.000000','1023.000000','2047.000000','4095.000000','8191.000000','16383.000000','32767.000000','65535.000000','131071.000000','262143.000000','524287.000000','1048575.000000','2097151.000000','4194303.000000','8388607.000000','16777215.000000','33554431.000000','67108863.000000','134217727.000000','268435455.000000','+Inf'])], 'vectorized_kafka_rpc_dispatch_handler_latency_bucket', 'kafka_rpc: Latency ', 'histogram'),
    m([ShardGen()], 'vectorized_kafka_rpc_fetch_avail_mem_bytes', 'kafka_rpc: Memory available for fetch request processing', 'counter'),
    m([ShardGen()], 'vectorized_kafka_rpc_max_service_mem_bytes', 'kafka_rpc: Maximum memory allowed for RPC', 'counter'),
    m([], 'vectorized_kafka_rpc_method_not_found_errors', 'kafka_rpc: Number of requests with not available RPC method', 'counter'),
    m([], 'vectorized_kafka_rpc_received_bytes', 'kafka_rpc: Number of bytes received from the clients in valid requests', 'counter'),
    m([], 'vectorized_kafka_rpc_requests_blocked_memory', 'kafka_rpc: Number of requests blocked in memory backpressure', 'counter'),
    m([], 'vectorized_kafka_rpc_requests_completed', 'kafka_rpc: Number of successful requests', 'counter'),
    m([], 'vectorized_kafka_rpc_requests_pending', 'kafka_rpc: Number of requests pending in the queue', 'gauge'),
    m([], 'vectorized_kafka_rpc_sent_bytes', 'kafka_rpc: Number of bytes sent to clients', 'counter'),
    m([], 'vectorized_kafka_rpc_service_errors', 'kafka_rpc: Number of service errors', 'counter'),
    m([], 'vectorized_kafka_schema_id_cache_batches_decompressed', 'Total number of batches decompressed for server-side schema ID validation', 'counter'),
    m([], 'vectorized_kafka_schema_id_cache_hits', 'Total number of hits for the server-side schema ID validation cache (see cluster config: kafka_schema_id_validation_cache_capacity)', 'counter'),
    m([], 'vectorized_kafka_schema_id_cache_misses', 'Total number of misses for the server-side schema ID validation cache (see cluster config: kafka_schema_id_validation_cache_capacity)', 'counter'),
    m([ShardGen()], 'vectorized_leader_balancer_leader_transfer_error', 'Number of errors attempting to transfer leader', 'counter'),
    m([ShardGen()], 'vectorized_leader_balancer_leader_transfer_no_improvement', 'Number of times no balance improvement was found', 'counter'),
    m([ShardGen()], 'vectorized_leader_balancer_leader_transfer_succeeded', 'Number of successful leader transfers', 'counter'),
    m([ShardGen()], 'vectorized_leader_balancer_leader_transfer_timeout', 'Number of timeouts attempting to transfer leader', 'counter'),
    m([ShardGen()], 'vectorized_memory_allocated_memory', 'Allocated memory size in bytes', 'gauge'),
    m([ShardGen()], 'vectorized_memory_available_memory', 'Total shard memory potentially available in bytes (free_memory plus reclaimable)', 'gauge'),
    m([ShardGen()], 'vectorized_memory_available_memory_low_water_mark', 'The low-water mark for available_memory from process start', 'gauge'),
    m([ShardGen()], 'vectorized_memory_cross_cpu_free_operations', 'Total number of cross cpu free', 'counter'),
    m([ShardGen()], 'vectorized_memory_free_memory', 'Free memory size in bytes', 'gauge'),
    m([ShardGen()], 'vectorized_memory_free_operations', 'Total number of free operations', 'counter'),
    m([ShardGen()], 'vectorized_memory_malloc_failed', 'Total count of failed memory allocations', 'counter'),
    m([ShardGen()], 'vectorized_memory_malloc_live_objects', 'Number of live objects', 'gauge'),
    m([ShardGen()], 'vectorized_memory_malloc_operations', 'Total number of malloc operations', 'counter'),
    m([ShardGen()], 'vectorized_memory_reclaims_operations', 'Total reclaims operations', 'counter'),
    m([ShardGen()], 'vectorized_memory_total_memory', 'Total memory size in bytes', 'gauge'),
    m([], 'vectorized_node_status_rpcs_received', 'Number of node status RPCs received by this node', 'gauge'),
    m([], 'vectorized_node_status_rpcs_sent', 'Number of node status RPCs sent by this node', 'gauge'),
    m([], 'vectorized_node_status_rpcs_timed_out', 'Number of timed out node status RPCs from this node', 'gauge'),
    m([PartitionGen(has_shard=False)], 'vectorized_ntp_archiver_missing', 'Missing offsets due to gaps', 'counter'),
    m([PartitionGen(has_shard=False)], 'vectorized_ntp_archiver_pending', 'Pending offsets', 'gauge'),
    m([PartitionGen(has_shard=False)], 'vectorized_ntp_archiver_uploaded', 'Uploaded offsets', 'counter'),
    m([PartitionGen(has_shard=False)], 'vectorized_ntp_archiver_uploaded_bytes', 'Total number of uploaded bytes', 'counter'),
    m([], 'vectorized_pandaproxy_request_latency_sum', 'Request latency', 'histogram'),
    m([], 'vectorized_pandaproxy_request_latency_count', 'Request latency', 'histogram'),
    m([ListGen('le', ['7.000000','15.000000','31.000000','63.000000','127.000000','255.000000','511.000000','1023.000000','2047.000000','4095.000000','8191.000000','16383.000000','32767.000000','65535.000000','131071.000000','262143.000000','524287.000000','1048575.000000','2097151.000000','4194303.000000','8388607.000000','16777215.000000','33554431.000000','67108863.000000','134217727.000000','268435455.000000','+Inf'])], 'vectorized_pandaproxy_request_latency_bucket', 'Request latency', 'histogram'),
    m([PartitionGen(has_shard=False)], 'vectorized_raft_configuration_change_in_progress', 'Indicates if current raft group configuration is in joint state i.e. configuration is being changed', 'gauge'),
    m([TopicGen(has_shard=False)], 'vectorized_raft_done_replicate_requests', 'Number of finished replicate requests', 'counter'),
    m([TopicGen(has_shard=False)], 'vectorized_raft_group_configuration_updates', 'Number of raft group configuration updates', 'counter'),
    m([ShardGen()], 'vectorized_raft_group_count', 'Number of raft groups', 'gauge'),
    m([TopicGen(has_shard=False)], 'vectorized_raft_heartbeat_requests_errors', 'Number of failed heartbeat requests', 'counter'),
    m([PartitionGen(has_shard=False)], 'vectorized_raft_leader_for', 'Number of groups for which node is a leader', 'gauge'),
    m([TopicGen(has_shard=False)], 'vectorized_raft_leadership_changes', 'Number of leadership changes', 'counter'),
    m([TopicGen(has_shard=False)], 'vectorized_raft_log_flushes', 'Number of log flushes', 'counter'),
    m([TopicGen(has_shard=False)], 'vectorized_raft_log_truncations', 'Number of log truncations', 'counter'),
    m([TopicGen(has_shard=False)], 'vectorized_raft_received_append_requests', 'Number of append requests received', 'counter'),
    m([TopicGen(has_shard=False)], 'vectorized_raft_received_vote_requests', 'Number of vote requests received', 'counter'),
    m([ShardGen()], 'vectorized_raft_recovery_partition_movement_assigned_bandwidth', 'Bandwidth assigned for partition movement in last tick. bytes/sec', 'gauge'),
    m([ShardGen()], 'vectorized_raft_recovery_partition_movement_available_bandwidth', 'Bandwidth available for partition movement. bytes/sec', 'gauge'),
    m([TopicGen(has_shard=False)], 'vectorized_raft_recovery_requests', 'Number of recovery requests', 'counter'),
    m([TopicGen(has_shard=False)], 'vectorized_raft_recovery_requests_errors', 'Number of failed recovery requests', 'counter'),
    m([TopicGen(has_shard=False)], 'vectorized_raft_replicate_ack_all_requests', 'Number of replicate requests with quorum ack consistency', 'counter'),
    m([TopicGen(has_shard=False)], 'vectorized_raft_replicate_ack_leader_requests', 'Number of replicate requests with leader ack consistency', 'counter'),
    m([TopicGen(has_shard=False)], 'vectorized_raft_replicate_ack_none_requests', 'Number of replicate requests with no ack consistency', 'counter'),
    m([TopicGen(has_shard=False)], 'vectorized_raft_replicate_batch_flush_requests', 'Number of replicate batch flushes', 'counter'),
    m([TopicGen(has_shard=False)], 'vectorized_raft_replicate_request_errors', 'Number of failed replicate requests', 'counter'),
    m([TopicGen(has_shard=False)], 'vectorized_raft_sent_vote_requests', 'Number of vote requests sent', 'counter'),
    m([ShardGen()], 'vectorized_reactor_abandoned_failed_futures', 'Total number of abandoned failed futures, futures destroyed while still containing an exception', 'counter'),
    m([ShardGen()], 'vectorized_reactor_aio_bytes_read', 'Total aio-reads bytes', 'counter'),
    m([ShardGen()], 'vectorized_reactor_aio_bytes_write', 'Total aio-writes bytes', 'counter'),
    m([ShardGen()], 'vectorized_reactor_aio_errors', 'Total aio errors', 'counter'),
    m([ShardGen()], 'vectorized_reactor_aio_outsizes', 'Total number of aio operations that exceed IO limit', 'counter'),
    m([ShardGen()], 'vectorized_reactor_aio_reads', 'Total aio-reads operations', 'counter'),
    m([ShardGen()], 'vectorized_reactor_aio_writes', 'Total aio-writes operations', 'counter'),
    m([ShardGen()], 'vectorized_reactor_cpp_exceptions', 'Total number of C++ exceptions', 'counter'),
    m([ShardGen()], 'vectorized_reactor_cpu_busy_ms', 'Total cpu busy time in milliseconds', 'counter'),
    m([ShardGen()], 'vectorized_reactor_cpu_steal_time_ms', "Total steal time, the time in which some other process was running while Seastar was not trying to run (not sleeping).Because this is in userspace, some time that could be legitimally thought as steal time is not accounted as such. For example, if we are sleeping and can wake up but the kernel hasn't woken us up yet.", 'counter'),
    m([ShardGen()], 'vectorized_reactor_fstream_read_bytes', 'Counts bytes read from disk file streams.  A high rate indicates high disk activity. Divide by fstream_reads to determine average read size.', 'counter'),
    m([ShardGen()], 'vectorized_reactor_fstream_read_bytes_blocked', 'Counts the number of bytes read from disk that could not be satisfied from read-ahead buffers, and had to block. Indicates short streams, or incorrect read ahead configuration.', 'counter'),
    m([ShardGen()], 'vectorized_reactor_fstream_reads', 'Counts reads from disk file streams.  A high rate indicates high disk activity. Contrast with other fstream_read* counters to locate bottlenecks.', 'counter'),
    m([ShardGen()], 'vectorized_reactor_fstream_reads_ahead_bytes_discarded', 'Counts the number of buffered bytes that were read ahead of time and were discarded because they were not needed, wasting disk bandwidth. Indicates over-eager read ahead configuration.', 'counter'),
    m([ShardGen()], 'vectorized_reactor_fstream_reads_aheads_discarded', 'Counts the number of times a buffer that was read ahead of time and was discarded because it was not needed, wasting disk bandwidth. Indicates over-eager read ahead configuration.', 'counter'),
    m([ShardGen()], 'vectorized_reactor_fstream_reads_blocked', 'Counts the number of times a disk read could not be satisfied from read-ahead buffers, and had to block. Indicates short streams, or incorrect read ahead configuration.', 'counter'),
    m([ShardGen()], 'vectorized_reactor_fsyncs', 'Total number of fsync operations', 'counter'),
    m([ShardGen()], 'vectorized_reactor_io_threaded_fallbacks', 'Total number of io-threaded-fallbacks operations', 'counter'),
    m([ShardGen()], 'vectorized_reactor_logging_failures', 'Total number of logging failures', 'counter'),
    m([ShardGen()], 'vectorized_reactor_polls', 'Number of times pollers were executed', 'counter'),
    m([ShardGen()], 'vectorized_reactor_tasks_pending', 'Number of pending tasks in the queue', 'gauge'),
    m([ShardGen()], 'vectorized_reactor_tasks_processed', 'Total tasks processed', 'counter'),
    m([ShardGen()], 'vectorized_reactor_timers_pending', 'Number of tasks in the timer-pending queue', 'gauge'),
    m([ShardGen()], 'vectorized_reactor_utilization', 'CPU utilization', 'gauge'),
    m([ShardGen(), ListGen('connection_cache_label', ['node_status_backend']), ListGen('target', ['rp-cmfv58ida36ph20vrtfg-7.rp-cmfv58ida36ph20vrtfg.redpanda.svc.cluster.local.:33145','rp-cmfv58ida36ph20vrtfg-3.rp-cmfv58ida36ph20vrtfg.redpanda.svc.cluster.local.:33145','rp-cmfv58ida36ph20vrtfg-2.rp-cmfv58ida36ph20vrtfg.redpanda.svc.cluster.local.:33145','rp-cmfv58ida36ph20vrtfg-8.rp-cmfv58ida36ph20vrtfg.redpanda.svc.cluster.local.:33145','rp-cmfv58ida36ph20vrtfg-5.rp-cmfv58ida36ph20vrtfg.redpanda.svc.cluster.local.:33145','rp-cmfv58ida36ph20vrtfg-1.rp-cmfv58ida36ph20vrtfg.redpanda.svc.cluster.local.:33145','rp-cmfv58ida36ph20vrtfg-0.rp-cmfv58ida36ph20vrtfg.redpanda.svc.cluster.local.:33145','rp-cmfv58ida36ph20vrtfg-6.rp-cmfv58ida36ph20vrtfg.redpanda.svc.cluster.local.:33145'])], 'vectorized_rpc_client_active_connections', 'Currently active connections', 'gauge'),
    m([ShardGen(), ListGen('connection_cache_label', ['node_status_backend']), ListGen('target', ['rp-cmfv58ida36ph20vrtfg-7.rp-cmfv58ida36ph20vrtfg.redpanda.svc.cluster.local.:33145','rp-cmfv58ida36ph20vrtfg-3.rp-cmfv58ida36ph20vrtfg.redpanda.svc.cluster.local.:33145','rp-cmfv58ida36ph20vrtfg-2.rp-cmfv58ida36ph20vrtfg.redpanda.svc.cluster.local.:33145','rp-cmfv58ida36ph20vrtfg-8.rp-cmfv58ida36ph20vrtfg.redpanda.svc.cluster.local.:33145','rp-cmfv58ida36ph20vrtfg-5.rp-cmfv58ida36ph20vrtfg.redpanda.svc.cluster.local.:33145','rp-cmfv58ida36ph20vrtfg-1.rp-cmfv58ida36ph20vrtfg.redpanda.svc.cluster.local.:33145','rp-cmfv58ida36ph20vrtfg-0.rp-cmfv58ida36ph20vrtfg.redpanda.svc.cluster.local.:33145','rp-cmfv58ida36ph20vrtfg-6.rp-cmfv58ida36ph20vrtfg.redpanda.svc.cluster.local.:33145'])], 'vectorized_rpc_client_client_correlation_errors', 'Number of errors in client correlation id', 'counter'),
    m([ShardGen(), ListGen('connection_cache_label', ['node_status_backend']), ListGen('target', ['rp-cmfv58ida36ph20vrtfg-7.rp-cmfv58ida36ph20vrtfg.redpanda.svc.cluster.local.:33145','rp-cmfv58ida36ph20vrtfg-3.rp-cmfv58ida36ph20vrtfg.redpanda.svc.cluster.local.:33145','rp-cmfv58ida36ph20vrtfg-2.rp-cmfv58ida36ph20vrtfg.redpanda.svc.cluster.local.:33145','rp-cmfv58ida36ph20vrtfg-8.rp-cmfv58ida36ph20vrtfg.redpanda.svc.cluster.local.:33145','rp-cmfv58ida36ph20vrtfg-5.rp-cmfv58ida36ph20vrtfg.redpanda.svc.cluster.local.:33145','rp-cmfv58ida36ph20vrtfg-1.rp-cmfv58ida36ph20vrtfg.redpanda.svc.cluster.local.:33145','rp-cmfv58ida36ph20vrtfg-0.rp-cmfv58ida36ph20vrtfg.redpanda.svc.cluster.local.:33145','rp-cmfv58ida36ph20vrtfg-6.rp-cmfv58ida36ph20vrtfg.redpanda.svc.cluster.local.:33145'])], 'vectorized_rpc_client_connection_errors', 'Number of connection errors', 'counter'),
    m([ShardGen(), ListGen('connection_cache_label', ['node_status_backend']), ListGen('target', ['rp-cmfv58ida36ph20vrtfg-7.rp-cmfv58ida36ph20vrtfg.redpanda.svc.cluster.local.:33145','rp-cmfv58ida36ph20vrtfg-3.rp-cmfv58ida36ph20vrtfg.redpanda.svc.cluster.local.:33145','rp-cmfv58ida36ph20vrtfg-2.rp-cmfv58ida36ph20vrtfg.redpanda.svc.cluster.local.:33145','rp-cmfv58ida36ph20vrtfg-8.rp-cmfv58ida36ph20vrtfg.redpanda.svc.cluster.local.:33145','rp-cmfv58ida36ph20vrtfg-5.rp-cmfv58ida36ph20vrtfg.redpanda.svc.cluster.local.:33145','rp-cmfv58ida36ph20vrtfg-1.rp-cmfv58ida36ph20vrtfg.redpanda.svc.cluster.local.:33145','rp-cmfv58ida36ph20vrtfg-0.rp-cmfv58ida36ph20vrtfg.redpanda.svc.cluster.local.:33145','rp-cmfv58ida36ph20vrtfg-6.rp-cmfv58ida36ph20vrtfg.redpanda.svc.cluster.local.:33145'])], 'vectorized_rpc_client_connects', 'Connection attempts', 'counter'),
    m([ShardGen(), ListGen('connection_cache_label', ['node_status_backend']), ListGen('target', ['rp-cmfv58ida36ph20vrtfg-7.rp-cmfv58ida36ph20vrtfg.redpanda.svc.cluster.local.:33145','rp-cmfv58ida36ph20vrtfg-3.rp-cmfv58ida36ph20vrtfg.redpanda.svc.cluster.local.:33145','rp-cmfv58ida36ph20vrtfg-2.rp-cmfv58ida36ph20vrtfg.redpanda.svc.cluster.local.:33145','rp-cmfv58ida36ph20vrtfg-8.rp-cmfv58ida36ph20vrtfg.redpanda.svc.cluster.local.:33145','rp-cmfv58ida36ph20vrtfg-5.rp-cmfv58ida36ph20vrtfg.redpanda.svc.cluster.local.:33145','rp-cmfv58ida36ph20vrtfg-1.rp-cmfv58ida36ph20vrtfg.redpanda.svc.cluster.local.:33145','rp-cmfv58ida36ph20vrtfg-0.rp-cmfv58ida36ph20vrtfg.redpanda.svc.cluster.local.:33145','rp-cmfv58ida36ph20vrtfg-6.rp-cmfv58ida36ph20vrtfg.redpanda.svc.cluster.local.:33145'])], 'vectorized_rpc_client_corrupted_headers', 'Number of responses with corrupted headers', 'counter'),
    m([ShardGen(), ListGen('connection_cache_label', ['node_status_backend']), ListGen('target', ['rp-cmfv58ida36ph20vrtfg-7.rp-cmfv58ida36ph20vrtfg.redpanda.svc.cluster.local.:33145','rp-cmfv58ida36ph20vrtfg-3.rp-cmfv58ida36ph20vrtfg.redpanda.svc.cluster.local.:33145','rp-cmfv58ida36ph20vrtfg-2.rp-cmfv58ida36ph20vrtfg.redpanda.svc.cluster.local.:33145','rp-cmfv58ida36ph20vrtfg-8.rp-cmfv58ida36ph20vrtfg.redpanda.svc.cluster.local.:33145','rp-cmfv58ida36ph20vrtfg-5.rp-cmfv58ida36ph20vrtfg.redpanda.svc.cluster.local.:33145','rp-cmfv58ida36ph20vrtfg-1.rp-cmfv58ida36ph20vrtfg.redpanda.svc.cluster.local.:33145','rp-cmfv58ida36ph20vrtfg-0.rp-cmfv58ida36ph20vrtfg.redpanda.svc.cluster.local.:33145','rp-cmfv58ida36ph20vrtfg-6.rp-cmfv58ida36ph20vrtfg.redpanda.svc.cluster.local.:33145'])], 'vectorized_rpc_client_in_bytes', 'Total number of bytes sent (including headers)', 'counter'),
    m([ShardGen(), ListGen('connection_cache_label', ['node_status_backend']), ListGen('target', ['rp-cmfv58ida36ph20vrtfg-7.rp-cmfv58ida36ph20vrtfg.redpanda.svc.cluster.local.:33145','rp-cmfv58ida36ph20vrtfg-3.rp-cmfv58ida36ph20vrtfg.redpanda.svc.cluster.local.:33145','rp-cmfv58ida36ph20vrtfg-2.rp-cmfv58ida36ph20vrtfg.redpanda.svc.cluster.local.:33145','rp-cmfv58ida36ph20vrtfg-8.rp-cmfv58ida36ph20vrtfg.redpanda.svc.cluster.local.:33145','rp-cmfv58ida36ph20vrtfg-5.rp-cmfv58ida36ph20vrtfg.redpanda.svc.cluster.local.:33145','rp-cmfv58ida36ph20vrtfg-1.rp-cmfv58ida36ph20vrtfg.redpanda.svc.cluster.local.:33145','rp-cmfv58ida36ph20vrtfg-0.rp-cmfv58ida36ph20vrtfg.redpanda.svc.cluster.local.:33145','rp-cmfv58ida36ph20vrtfg-6.rp-cmfv58ida36ph20vrtfg.redpanda.svc.cluster.local.:33145'])], 'vectorized_rpc_client_out_bytes', 'Total number of bytes received', 'counter'),
    m([ShardGen(), ListGen('connection_cache_label', ['node_status_backend']), ListGen('target', ['rp-cmfv58ida36ph20vrtfg-7.rp-cmfv58ida36ph20vrtfg.redpanda.svc.cluster.local.:33145','rp-cmfv58ida36ph20vrtfg-3.rp-cmfv58ida36ph20vrtfg.redpanda.svc.cluster.local.:33145','rp-cmfv58ida36ph20vrtfg-2.rp-cmfv58ida36ph20vrtfg.redpanda.svc.cluster.local.:33145','rp-cmfv58ida36ph20vrtfg-8.rp-cmfv58ida36ph20vrtfg.redpanda.svc.cluster.local.:33145','rp-cmfv58ida36ph20vrtfg-5.rp-cmfv58ida36ph20vrtfg.redpanda.svc.cluster.local.:33145','rp-cmfv58ida36ph20vrtfg-1.rp-cmfv58ida36ph20vrtfg.redpanda.svc.cluster.local.:33145','rp-cmfv58ida36ph20vrtfg-0.rp-cmfv58ida36ph20vrtfg.redpanda.svc.cluster.local.:33145','rp-cmfv58ida36ph20vrtfg-6.rp-cmfv58ida36ph20vrtfg.redpanda.svc.cluster.local.:33145'])], 'vectorized_rpc_client_read_dispatch_errors', 'Number of errors while dispatching responses', 'counter'),
    m([ShardGen(), ListGen('connection_cache_label', ['node_status_backend']), ListGen('target', ['rp-cmfv58ida36ph20vrtfg-7.rp-cmfv58ida36ph20vrtfg.redpanda.svc.cluster.local.:33145','rp-cmfv58ida36ph20vrtfg-3.rp-cmfv58ida36ph20vrtfg.redpanda.svc.cluster.local.:33145','rp-cmfv58ida36ph20vrtfg-2.rp-cmfv58ida36ph20vrtfg.redpanda.svc.cluster.local.:33145','rp-cmfv58ida36ph20vrtfg-8.rp-cmfv58ida36ph20vrtfg.redpanda.svc.cluster.local.:33145','rp-cmfv58ida36ph20vrtfg-5.rp-cmfv58ida36ph20vrtfg.redpanda.svc.cluster.local.:33145','rp-cmfv58ida36ph20vrtfg-1.rp-cmfv58ida36ph20vrtfg.redpanda.svc.cluster.local.:33145','rp-cmfv58ida36ph20vrtfg-0.rp-cmfv58ida36ph20vrtfg.redpanda.svc.cluster.local.:33145','rp-cmfv58ida36ph20vrtfg-6.rp-cmfv58ida36ph20vrtfg.redpanda.svc.cluster.local.:33145'])], 'vectorized_rpc_client_request_errors', 'Number or requests errors', 'counter'),
    m([ShardGen(), ListGen('connection_cache_label', ['node_status_backend']), ListGen('target', ['rp-cmfv58ida36ph20vrtfg-7.rp-cmfv58ida36ph20vrtfg.redpanda.svc.cluster.local.:33145','rp-cmfv58ida36ph20vrtfg-3.rp-cmfv58ida36ph20vrtfg.redpanda.svc.cluster.local.:33145','rp-cmfv58ida36ph20vrtfg-2.rp-cmfv58ida36ph20vrtfg.redpanda.svc.cluster.local.:33145','rp-cmfv58ida36ph20vrtfg-8.rp-cmfv58ida36ph20vrtfg.redpanda.svc.cluster.local.:33145','rp-cmfv58ida36ph20vrtfg-5.rp-cmfv58ida36ph20vrtfg.redpanda.svc.cluster.local.:33145','rp-cmfv58ida36ph20vrtfg-1.rp-cmfv58ida36ph20vrtfg.redpanda.svc.cluster.local.:33145','rp-cmfv58ida36ph20vrtfg-0.rp-cmfv58ida36ph20vrtfg.redpanda.svc.cluster.local.:33145','rp-cmfv58ida36ph20vrtfg-6.rp-cmfv58ida36ph20vrtfg.redpanda.svc.cluster.local.:33145'])], 'vectorized_rpc_client_request_timeouts', 'Number or requests timeouts', 'counter'),
    m([ShardGen(), ListGen('connection_cache_label', ['node_status_backend']), ListGen('target', ['rp-cmfv58ida36ph20vrtfg-7.rp-cmfv58ida36ph20vrtfg.redpanda.svc.cluster.local.:33145','rp-cmfv58ida36ph20vrtfg-3.rp-cmfv58ida36ph20vrtfg.redpanda.svc.cluster.local.:33145','rp-cmfv58ida36ph20vrtfg-2.rp-cmfv58ida36ph20vrtfg.redpanda.svc.cluster.local.:33145','rp-cmfv58ida36ph20vrtfg-8.rp-cmfv58ida36ph20vrtfg.redpanda.svc.cluster.local.:33145','rp-cmfv58ida36ph20vrtfg-5.rp-cmfv58ida36ph20vrtfg.redpanda.svc.cluster.local.:33145','rp-cmfv58ida36ph20vrtfg-1.rp-cmfv58ida36ph20vrtfg.redpanda.svc.cluster.local.:33145','rp-cmfv58ida36ph20vrtfg-0.rp-cmfv58ida36ph20vrtfg.redpanda.svc.cluster.local.:33145','rp-cmfv58ida36ph20vrtfg-6.rp-cmfv58ida36ph20vrtfg.redpanda.svc.cluster.local.:33145'])], 'vectorized_rpc_client_requests', 'Number of requests', 'counter'),
    m([ShardGen(), ListGen('connection_cache_label', ['node_status_backend']), ListGen('target', ['rp-cmfv58ida36ph20vrtfg-7.rp-cmfv58ida36ph20vrtfg.redpanda.svc.cluster.local.:33145','rp-cmfv58ida36ph20vrtfg-3.rp-cmfv58ida36ph20vrtfg.redpanda.svc.cluster.local.:33145','rp-cmfv58ida36ph20vrtfg-2.rp-cmfv58ida36ph20vrtfg.redpanda.svc.cluster.local.:33145','rp-cmfv58ida36ph20vrtfg-8.rp-cmfv58ida36ph20vrtfg.redpanda.svc.cluster.local.:33145','rp-cmfv58ida36ph20vrtfg-5.rp-cmfv58ida36ph20vrtfg.redpanda.svc.cluster.local.:33145','rp-cmfv58ida36ph20vrtfg-1.rp-cmfv58ida36ph20vrtfg.redpanda.svc.cluster.local.:33145','rp-cmfv58ida36ph20vrtfg-0.rp-cmfv58ida36ph20vrtfg.redpanda.svc.cluster.local.:33145','rp-cmfv58ida36ph20vrtfg-6.rp-cmfv58ida36ph20vrtfg.redpanda.svc.cluster.local.:33145'])], 'vectorized_rpc_client_requests_blocked_memory', 'Number of requests that are blocked because of insufficient memory', 'counter'),
    m([ShardGen(), ListGen('connection_cache_label', ['node_status_backend']), ListGen('target', ['rp-cmfv58ida36ph20vrtfg-7.rp-cmfv58ida36ph20vrtfg.redpanda.svc.cluster.local.:33145','rp-cmfv58ida36ph20vrtfg-3.rp-cmfv58ida36ph20vrtfg.redpanda.svc.cluster.local.:33145','rp-cmfv58ida36ph20vrtfg-2.rp-cmfv58ida36ph20vrtfg.redpanda.svc.cluster.local.:33145','rp-cmfv58ida36ph20vrtfg-8.rp-cmfv58ida36ph20vrtfg.redpanda.svc.cluster.local.:33145','rp-cmfv58ida36ph20vrtfg-5.rp-cmfv58ida36ph20vrtfg.redpanda.svc.cluster.local.:33145','rp-cmfv58ida36ph20vrtfg-1.rp-cmfv58ida36ph20vrtfg.redpanda.svc.cluster.local.:33145','rp-cmfv58ida36ph20vrtfg-0.rp-cmfv58ida36ph20vrtfg.redpanda.svc.cluster.local.:33145','rp-cmfv58ida36ph20vrtfg-6.rp-cmfv58ida36ph20vrtfg.redpanda.svc.cluster.local.:33145'])], 'vectorized_rpc_client_requests_pending', 'Number of requests pending', 'gauge'),
    m([ShardGen(), ListGen('connection_cache_label', ['node_status_backend']), ListGen('target', ['rp-cmfv58ida36ph20vrtfg-7.rp-cmfv58ida36ph20vrtfg.redpanda.svc.cluster.local.:33145','rp-cmfv58ida36ph20vrtfg-3.rp-cmfv58ida36ph20vrtfg.redpanda.svc.cluster.local.:33145','rp-cmfv58ida36ph20vrtfg-2.rp-cmfv58ida36ph20vrtfg.redpanda.svc.cluster.local.:33145','rp-cmfv58ida36ph20vrtfg-8.rp-cmfv58ida36ph20vrtfg.redpanda.svc.cluster.local.:33145','rp-cmfv58ida36ph20vrtfg-5.rp-cmfv58ida36ph20vrtfg.redpanda.svc.cluster.local.:33145','rp-cmfv58ida36ph20vrtfg-1.rp-cmfv58ida36ph20vrtfg.redpanda.svc.cluster.local.:33145','rp-cmfv58ida36ph20vrtfg-0.rp-cmfv58ida36ph20vrtfg.redpanda.svc.cluster.local.:33145','rp-cmfv58ida36ph20vrtfg-6.rp-cmfv58ida36ph20vrtfg.redpanda.svc.cluster.local.:33145'])], 'vectorized_rpc_client_server_correlation_errors', 'Number of responses with wrong correlation id', 'counter'),
    m([ShardGen(), ListGen('group', ['admin','archival_upload','atexit','cache_background_reclaim','cluster','fetch','kafka','log_compaction','main','node_status','raft','raft_learner_recovery','self_test'])], 'vectorized_scheduler_queue_length', 'Size of backlog on this queue, in tasks; indicates whether the queue is busy and/or contended', 'gauge'),
    m([ShardGen(), ListGen('group', ['admin','archival_upload','atexit','cache_background_reclaim','cluster','fetch','kafka','log_compaction','main','node_status','raft','raft_learner_recovery','self_test'])], 'vectorized_scheduler_runtime_ms', 'Accumulated runtime of this task queue; an increment rate of 1000ms per second indicates full utilization', 'counter'),
    m([ShardGen(), ListGen('group', ['admin','archival_upload','atexit','cache_background_reclaim','cluster','fetch','kafka','log_compaction','main','node_status','raft','raft_learner_recovery','self_test'])], 'vectorized_scheduler_shares', 'Shares allocated to this queue', 'gauge'),
    m([ShardGen(), ListGen('group', ['admin','archival_upload','atexit','cache_background_reclaim','cluster','fetch','kafka','log_compaction','main','node_status','raft','raft_learner_recovery','self_test'])], 'vectorized_scheduler_starvetime_ms', 'Accumulated starvation time of this task queue; an increment rate of 1000ms per second indicates the scheduler feels really bad', 'counter'),
    m([ShardGen(), ListGen('group', ['admin','archival_upload','atexit','cache_background_reclaim','cluster','fetch','kafka','log_compaction','main','node_status','raft','raft_learner_recovery','self_test'])], 'vectorized_scheduler_tasks_processed', 'Count of tasks executing on this queue; indicates together with runtime_ms indicates length of tasks', 'counter'),
    m([ShardGen(), ListGen('group', ['admin','archival_upload','atexit','cache_background_reclaim','cluster','fetch','kafka','log_compaction','main','node_status','raft','raft_learner_recovery','self_test'])], 'vectorized_scheduler_time_spent_on_task_quota_violations_ms', 'Total amount in milliseconds we were in violation of the task quota', 'counter'),
    m([ShardGen(), ListGen('group', ['admin','archival_upload','atexit','cache_background_reclaim','cluster','fetch','kafka','log_compaction','main','node_status','raft','raft_learner_recovery','self_test'])], 'vectorized_scheduler_waittime_ms', 'Accumulated waittime of this task queue; an increment rate of 1000ms per second indicates queue is waiting for something (e.g. IO)', 'counter'),
    m([ShardGen()], 'vectorized_stall_detector_reported', 'Total number of reported stalls, look in the traces for the exact reason', 'counter'),
    m([ShardGen()], 'vectorized_storage_compaction_backlog_controller_backlog_size', 'controller backlog', 'gauge'),
    m([ShardGen()], 'vectorized_storage_compaction_backlog_controller_error', 'current controller error, i.e difference between set point and backlog size', 'gauge'),
    m([ShardGen()], 'vectorized_storage_compaction_backlog_controller_shares', 'controller output, i.e. number of shares', 'gauge'),
    m([ShardGen()], 'vectorized_storage_kvstore_cached_bytes', 'Size of the database in memory', 'gauge'),
    m([ShardGen()], 'vectorized_storage_kvstore_entries_fetched', 'Number of entries fetched', 'counter'),
    m([ShardGen()], 'vectorized_storage_kvstore_entries_removed', 'Number of entries removaled', 'counter'),
    m([ShardGen()], 'vectorized_storage_kvstore_entries_written', 'Number of entries written', 'counter'),
    m([ShardGen()], 'vectorized_storage_kvstore_key_count', 'Number of keys in the database', 'counter'),
    m([ShardGen()], 'vectorized_storage_kvstore_segments_rolled', 'Number of segments rolled', 'counter'),
    m([TopicGen(has_shard=False)], 'vectorized_storage_log_batch_parse_errors', 'Number of batch parsing (reading) errors', 'counter'),
    m([TopicGen(has_shard=False)], 'vectorized_storage_log_batch_write_errors', 'Number of batch write errors', 'counter'),
    m([TopicGen(has_shard=False)], 'vectorized_storage_log_batches_read', 'Total number of batches read', 'counter'),
    m([TopicGen(has_shard=False)], 'vectorized_storage_log_batches_written', 'Total number of batches written', 'counter'),
    m([TopicGen(has_shard=False)], 'vectorized_storage_log_cache_hits', 'Reader cache hits', 'counter'),
    m([TopicGen(has_shard=False)], 'vectorized_storage_log_cache_misses', 'Reader cache misses', 'counter'),
    m([TopicGen(has_shard=False)], 'vectorized_storage_log_cached_batches_read', 'Total number of cached batches read', 'counter'),
    m([TopicGen(has_shard=False)], 'vectorized_storage_log_cached_read_bytes', 'Total number of cached bytes read', 'counter'),
    m([TopicGen(has_shard=False)], 'vectorized_storage_log_compacted_segment', 'Number of compacted segments', 'counter'),
    m([PartitionGen(has_shard=False)], 'vectorized_storage_log_compaction_ratio', 'Average segment compaction ratio', 'counter'),
    m([TopicGen(has_shard=False)], 'vectorized_storage_log_corrupted_compaction_indices', 'Number of times we had to re-construct the .compaction index on a segment', 'counter'),
    m([TopicGen(has_shard=False)], 'vectorized_storage_log_log_segments_active', 'Current number of local log segments', 'counter'),
    m([TopicGen(has_shard=False)], 'vectorized_storage_log_log_segments_created', 'Total number of local log segments created since node startup', 'counter'),
    m([TopicGen(has_shard=False)], 'vectorized_storage_log_log_segments_removed', 'Total number of local log segments removed since node startup', 'counter'),
    m([TopicGen(has_shard=False)], 'vectorized_storage_log_partition_size', 'Current size of partition in bytes', 'gauge'),
    m([TopicGen(has_shard=False)], 'vectorized_storage_log_read_bytes', 'Total number of bytes read', 'counter'),
    m([TopicGen(has_shard=False)], 'vectorized_storage_log_readers_added', 'Number of readers added to cache', 'counter'),
    m([TopicGen(has_shard=False)], 'vectorized_storage_log_readers_evicted', 'Number of readers evicted from cache', 'counter'),
    m([TopicGen(has_shard=False)], 'vectorized_storage_log_written_bytes', 'Total number of bytes written', 'counter'),
    m([PartitionGen(has_shard=False)], 'vectorized_tx_partition_idempotency_num_pids_inflight', 'Number of pids with in flight idempotent produce requests.', 'gauge'),
    m([PartitionGen(has_shard=False)], 'vectorized_tx_partition_idempotency_pid_cache_size', 'Number of active producers (known producer_id seq number pairs).', 'gauge'),
    m([PartitionGen(has_shard=False)], 'vectorized_tx_partition_tx_mem_tracker_consumption_bytes', 'Total memory bytes in use by tx subsystem.', 'gauge'),
    m([PartitionGen(has_shard=False)], 'vectorized_tx_partition_tx_num_inflight_requests', 'Number of ongoing transactional requests.', 'gauge'),

]

PUBLIC_METRICS = [
    m([ListGen('redpanda_revision', ['fd1019bc7ee99e5821c645fe54c1c835c2188e69']), ListGen('redpanda_version', ['v23.2.21'])], 'redpanda_application_build', 'Redpanda build information', 'gauge'),
    m([], 'redpanda_application_uptime_seconds_total', 'Redpanda uptime in seconds', 'gauge'),
    m([ListGen('redpanda_cmd_group', ['node_management_operations','move_operations','configuration_operations','topic_operations','acls_and_users_operations'])], 'redpanda_cluster_controller_log_limit_requests_available_rps', 'Controller log rate limiting. Available rps for group', 'gauge'),
    m([ListGen('redpanda_cmd_group', ['node_management_operations','move_operations','configuration_operations','topic_operations','acls_and_users_operations'])], 'redpanda_cluster_controller_log_limit_requests_dropped', 'Controller log rate limiting. Amount of requests that are dropped due to exceeding limit in group', 'counter'),
    m([], 'redpanda_cluster_partition_moving_from_node', 'Amount of partitions that are moving from node', 'gauge'),
    m([], 'redpanda_cluster_partition_moving_to_node', 'Amount of partitions that are moving to node', 'gauge'),
    m([], 'redpanda_cluster_partition_node_cancelling_movements', 'Amount of cancelling partition movements for node', 'gauge'),
    m([], 'redpanda_cluster_partition_num_with_broken_rack_constraint', "Number of partitions that don't satisfy the rack awareness constraint", 'gauge'),
    m([ShardGen()], 'redpanda_cpu_busy_seconds_total', 'Total CPU busy time in seconds', 'gauge'),
    m([IOShardGen(), ListGen('class', ['compaction','default','kafka_read','raft']), ListGen('mountpoint', ['none'])], 'redpanda_io_queue_total_read_ops', 'Total read operations passed in the queue', 'counter'),
    m([IOShardGen(), ListGen('class', ['compaction','default','kafka_read','raft']), ListGen('mountpoint', ['none'])], 'redpanda_io_queue_total_write_ops', 'Total write operations passed in the queue', 'counter'),
    m([GroupOffsetGen()], 'redpanda_kafka_consumer_group_committed_offset', 'Consumer group committed offset', 'gauge'),
    m([ListGen('redpanda_group', ['group-0'])], 'redpanda_kafka_consumer_group_consumers', 'Number of consumers in a group', 'gauge'),
    m([ListGen('redpanda_group', ['group-0'])], 'redpanda_kafka_consumer_group_topics', 'Number of topics in a group', 'gauge'),
    m([ListGen('handler', ['produce','fetch'])], 'redpanda_kafka_handler_latency_seconds_sum', 'Latency histogram of kafka requests', 'histogram'),
    m([ListGen('handler', ['produce','fetch'])], 'redpanda_kafka_handler_latency_seconds_count', 'Latency histogram of kafka requests', 'histogram'),
    m([ListGen('handler', ['produce','fetch']), ListGen('le', ['0.000255','0.000511','0.001023','0.002047','0.004095','0.008191','0.016383','0.032767','0.065535','0.131071','0.262143','0.524287','1.048575','2.097151','4.194303','8.388607','16.777215','33.554431','+Inf'])], 'redpanda_kafka_handler_latency_seconds_bucket', 'Latency histogram of kafka requests', 'histogram'),
    m([PartitionGen(has_shard=False)], 'redpanda_kafka_max_offset', 'Latest readable offset of the partition (i.e. high watermark)', 'gauge'),
    m([TopicGen(has_shard=False)], 'redpanda_kafka_partitions', 'Configured number of partitions for the topic', 'gauge'),
    m([TopicGen(has_shard=False)], 'redpanda_kafka_records_fetched_total', 'Total number of records fetched', 'counter'),
    m([TopicGen(has_shard=False)], 'redpanda_kafka_records_produced_total', 'Total number of records produced', 'counter'),
    m([TopicGen(has_shard=False)], 'redpanda_kafka_replicas', 'Configured number of replicas for the topic', 'gauge'),
    m([TopicGen(has_shard=False), ListGen('redpanda_request', ['consume','produce'])], 'redpanda_kafka_request_bytes_total', 'Total number of bytes produced per topic', 'counter'),
    m([ListGen('redpanda_request', ['produce','consume'])], 'redpanda_kafka_request_latency_seconds_sum', 'Internal latency of kafka produce requests', 'histogram'),
    m([ListGen('redpanda_request', ['produce','consume'])], 'redpanda_kafka_request_latency_seconds_count', 'Internal latency of kafka produce requests', 'histogram'),
    m([ListGen('le', ['0.000255','0.000511','0.001023','0.002047','0.004095','0.008191','0.016383','0.032767','0.065535','0.131071','0.262143','0.524287','1.048575','2.097151','4.194303','8.388607','16.777215','33.554431','+Inf']), ListGen('redpanda_request', ['produce','consume'])], 'redpanda_kafka_request_latency_seconds_bucket', 'Internal latency of kafka produce requests', 'histogram'),
    m([PartitionGen(has_shard=False)], 'redpanda_kafka_under_replicated_replicas', 'Number of under replicated replicas (i.e. replicas that are live, but not at the latest offest)', 'gauge'),
    m([ShardGen()], 'redpanda_memory_allocated_memory', 'Allocated memory size in bytes', 'gauge'),
    m([ShardGen()], 'redpanda_memory_available_memory', 'Total shard memory potentially available in bytes (free_memory plus reclaimable)', 'gauge'),
    m([ShardGen()], 'redpanda_memory_available_memory_low_water_mark', 'The low-water mark for available_memory from process start', 'gauge'),
    m([ShardGen()], 'redpanda_memory_free_memory', 'Free memory size in bytes', 'gauge'),
    m([], 'redpanda_node_status_rpcs_received', 'Number of node status RPCs received by this node', 'gauge'),
    m([], 'redpanda_node_status_rpcs_sent', 'Number of node status RPCs sent by this node', 'gauge'),
    m([], 'redpanda_node_status_rpcs_timed_out', 'Number of timed out node status RPCs from this node', 'gauge'),
    m([TopicGen(has_shard=False)], 'redpanda_raft_leadership_changes', 'Number of leadership changes across all partitions of a given topic', 'counter'),
    m([ShardGen()], 'redpanda_raft_recovery_partition_movement_available_bandwidth', 'Bandwidth available for partition movement. bytes/sec', 'gauge'),
    m([ListGen('redpanda_status', ['5xx','4xx','3xx'])], 'redpanda_rest_proxy_request_errors_total', 'Total number of rest_proxy server errors', 'counter'),
    m([], 'redpanda_rest_proxy_request_latency_seconds_sum', 'Internal latency of request for rest_proxy', 'histogram'),
    m([], 'redpanda_rest_proxy_request_latency_seconds_count', 'Internal latency of request for rest_proxy', 'histogram'),
    m([ListGen('le', ['0.000255','0.000511','0.001023','0.002047','0.004095','0.008191','0.016383','0.032767','0.065535','0.131071','0.262143','0.524287','1.048575','2.097151','4.194303','8.388607','16.777215','33.554431','+Inf'])], 'redpanda_rest_proxy_request_latency_seconds_bucket', 'Internal latency of request for rest_proxy', 'histogram'),
    m([ListGen('redpanda_server', ['kafka','internal'])], 'redpanda_rpc_active_connections', 'Count of currently active connections', 'gauge'),
    m([ListGen('redpanda_server', ['kafka','internal'])], 'redpanda_rpc_received_bytes', 'internal: Number of bytes received from the clients in valid requests', 'counter'),
    m([ListGen('redpanda_server', ['kafka','internal'])], 'redpanda_rpc_request_errors_total', 'Number of rpc errors', 'counter'),
    m([ListGen('redpanda_server', ['kafka','internal'])], 'redpanda_rpc_request_latency_seconds_sum', 'RPC latency', 'histogram'),
    m([ListGen('redpanda_server', ['kafka','internal'])], 'redpanda_rpc_request_latency_seconds_count', 'RPC latency', 'histogram'),
    m([ListGen('le', ['0.000255','0.000511','0.001023','0.002047','0.004095','0.008191','0.016383','0.032767','0.065535','0.131071','0.262143','0.524287','1.048575','2.097151','4.194303','8.388607','16.777215','33.554431','+Inf']), ListGen('redpanda_server', ['kafka','internal'])], 'redpanda_rpc_request_latency_seconds_bucket', 'RPC latency', 'histogram'),
    m([ListGen('redpanda_server', ['kafka','internal'])], 'redpanda_rpc_sent_bytes', 'internal: Number of bytes sent to clients', 'counter'),
    m([ShardGen(), ListGen('redpanda_scheduling_group', ['admin','archival_upload','cache_background_reclaim','cluster','fetch','kafka','log_compaction','main','node_status','raft','raft_learner_recovery','self_test'])], 'redpanda_scheduler_runtime_seconds_total', 'Accumulated runtime of task queue associated with this scheduling group', 'counter'),
    m([ListGen('redpanda_status', ['5xx','4xx','3xx'])], 'redpanda_schema_registry_request_errors_total', 'Total number of schema_registry server errors', 'counter'),
    m([], 'redpanda_schema_registry_request_latency_seconds_sum', 'Internal latency of request for schema_registry', 'histogram'),
    m([], 'redpanda_schema_registry_request_latency_seconds_count', 'Internal latency of request for schema_registry', 'histogram'),
    m([ListGen('le', ['0.000255','0.000511','0.001023','0.002047','0.004095','0.008191','0.016383','0.032767','0.065535','0.131071','0.262143','0.524287','1.048575','2.097151','4.194303','8.388607','16.777215','33.554431','+Inf'])], 'redpanda_schema_registry_request_latency_seconds_bucket', 'Internal latency of request for schema_registry', 'histogram'),
    m([], 'redpanda_storage_disk_free_bytes', 'Disk storage bytes free.', 'gauge'),
    m([], 'redpanda_storage_disk_free_space_alert', 'Status of low storage space alert. 0-OK, 1-Low Space 2-Degraded', 'gauge'),
    m([], 'redpanda_storage_disk_total_bytes', 'Total size of attached storage, in bytes.', 'gauge'),

]
# yapf: enable

main(METRICS, PUBLIC_METRICS)
