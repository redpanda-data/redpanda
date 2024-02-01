#!/usr/bin/env python3

# probably not useful: utility to injest metrics in prometheus format and

from collections import defaultdict
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
import re
import argparse
import sys

# HELP vectorized_alien_receive_batch_queue_length Current receive batch queue length
help_regex = re.compile(r'# HELP (?P<name>[^ ]+) (?P<value>.*)')
# TYPE vectorized_alien_receive_batch_queue_length gauge
type_regex = re.compile(r'# TYPE (?P<name>[^ ]+) (?P<value>.*)')
# vectorized_alien_receive_batch_queue_length{shard="0"} 0.000000
sample_regex = re.compile(
    r'(?P<name>[^ ]+){(?P<labels>[^}]*)} (?P<value>[-0-9.]+)')
# namespace="kafka"
label_regex = re.compile(r'(?P<name>[a-zA-Z_][a-zA-Z0-9_]+)="(?P<value>.*)"')


@dataclass
class Label:
    name: str
    value: str


@dataclass
class MetricMeta:
    type: str | None = None
    help: str | None = None
    label_names: set[str] = field(default_factory=set)
    # number of times this metric occurs in all metrics
    count: int = 0


@dataclass
class Metric:
    name: str
    labels: list[Label]
    value: float
    meta: MetricMeta

    @property
    def label_names(self):
        as_list = [l.name for l in self.labels]
        as_set = set(as_list)
        assert len(as_set) == len(as_list)
        return as_set


def parse_labels(label_str: str) -> list[Label]:
    if not label_str:
        return []
    labels: list[Label] = []
    for label in label_str.split(','):
        m = label_regex.fullmatch(label)
        assert m, f"label did not match: {label}, label string: '{label_str}'"
        name, value = m.group('name', 'value')
        labels.append(Label(name, value))
    return labels


# in general we expect metrics with the same name to share the same
# label set (i.e, set of label names ... not values!) but there are
# some exceptions: if the labels which are outside the common set are
# in this list we don't complain
MISMATCHED_LABELS_ALLOW = ['connection_cache_label']

# labels which can just be handled generically, i.e., which independently
# multiply the cardinality of the remaining labels by their cardinality
# and which use the same literal values everywhere
_KNOWN_STANDALONE_LABELS = [
    'version', 'revision', 'endpoint', 'region', 'latency_metric',
    'fetch_result', 'service', 'class', 'mountpoint', 'stream', 'handler',
    'direction', 'connection_cache_label', 'target', 'group', 'le',
    'cmd_group', 'request', 'status', 'server', 'scheduling_group'
]


class Endpoint(Enum):
    INTERNAL = 'metrics'
    PUBLIC = 'public_metrics'


def add_prefix(name: str):
    return "redpanda_" + name


def strip_prefix(name: str):
    return name.removeprefix("redpanda_")


KNOWN_STANDALONE_LABELS = {
    Endpoint.INTERNAL:
    _KNOWN_STANDALONE_LABELS,
    # PUBLIC is a mix of redpanda_ prefixed and not label names, so just include
    # all in both states
    Endpoint.PUBLIC:
    _KNOWN_STANDALONE_LABELS + list(map(add_prefix, _KNOWN_STANDALONE_LABELS))
}


def basename(name: str):
    for s in ['_sum', '_count', '_bucket']:
        if name.endswith(s):
            return name.removesuffix(s)
    return name


def parse_metrics(path: Path, endpoint: Endpoint) -> list[str]:

    print(f'Parsing input file: {path}', file=sys.stderr)

    metrics: list[Metric] = []
    name_to_meta: defaultdict[str,
                              MetricMeta] = defaultdict(lambda: MetricMeta())

    def lookup_meta(name: str):
        if name in name_to_meta:
            return name_to_meta[name]

        # hookup histo variation
        basename = name
        for s in ('_sum', '_count', '_bucket'):
            basename = basename.removesuffix(s)

        if basename in name_to_meta:
            meta = name_to_meta[basename]
            assert meta.type == 'histogram'
            return meta

        raise RuntimeError(f'No meta for {name}, basename: {basename}')

    file = open(path)
    for line in file:
        if m := help_regex.match(line):
            name, value = m.group('name', 'value')
            assert name_to_meta[name].help is None, 'duplicate help'
            name_to_meta[name].help = value
        elif m := type_regex.match(line):
            name, value = m.group('name', 'value')
            assert name_to_meta[name].type is None, 'duplicate type'
            name_to_meta[name].type = value
        elif m := sample_regex.match(line):
            name, labels, value = m.group('name', 'labels', 'value')
            bname = basename(name)

            # we try to determine if this metric is a histogram using the
            # following approach: if the name is histogram-like (i.e. ends
            # with one of the histogram-like suffixes, like _sum or _count)
            # and there is populated meta with type histo then, it's a histo
            if bname != name and bname in name_to_meta and name_to_meta[
                    bname].type == 'histogram':
                pass
            else:
                bname = name

            meta = lookup_meta(name)

            # for histograms, we only include one copy for each unique label set, ignoring le
            # i.e., the ~20 underlying metrics (_count, _sum and the le="..." metrics) for 1 histogram metric
            # are only included once

            meta.count += 1
            metrics.append(
                Metric(name, parse_labels(labels), float(value), meta))
        else:
            print(f'UNMATCHED LINE: {line}')

    print(f'Total metrics: {len(metrics)}', file=sys.stderr)
    print(f'Total meta: {len(name_to_meta)}', file=sys.stderr)

    name_to_labels: dict[str, defaultdict[str, dict[str, None]]] = {}

    for m in metrics:
        # output a line suitable to regenerate the metric
        label_set = m.label_names

        existing = name_to_labels.get(m.name)
        if existing is not None:
            diff = set(existing.keys()).symmetric_difference(label_set)
            for d in diff:
                assert d in MISMATCHED_LABELS_ALLOW, f'metric {m.name} had different label sets:\n{existing}\n{label_set}'
        else:
            name_to_labels[m.name] = defaultdict(dict)
            existing = name_to_labels[m.name]

        for l in m.labels:
            existing[l.name][l.value] = None

    print(f'Total names_to_labels: {len(name_to_labels)}', file=sys.stderr)

    known_labels = KNOWN_STANDALONE_LABELS[endpoint]

    lines: list[str] = []
    for mname, labels in name_to_labels.items():

        gens: list[str] = []
        special = sorted(
            map(strip_prefix,
                set(labels.keys()).difference(known_labels)))

        fully_handled = False

        if not special:
            pass
        elif special == ['shard']:
            gens.append('ShardGen()')
        elif special == ['ioshard', 'shard']:
            gens.append('IOShardGen()')
        else:
            has_shard = False
            if 'shard' in special:
                has_shard = True
                special.remove('shard')

            if special == ['namespace', 'topic']:
                gens.append(f'TopicGen(has_shard={has_shard})')
            elif special == ['namespace', 'partition', 'topic']:
                gens.append(f'PartitionGen(has_shard={has_shard})')
            elif special == ['partition', 'topic']:
                # This is for vectorized_kafka_group_offset which is a special case
                # we fully handle it here
                if endpoint == Endpoint.INTERNAL:
                    assert labels.keys() == {
                        'topic', 'partition', 'group', 'shard'
                    }, f'vectorized_kafka_group_offset-alike had wrong keys: {labels.keys()}'
                else:
                    assert labels.keys() == {
                        'redpanda_topic', 'redpanda_partition',
                        'redpanda_group'
                    }, f'redpanda_kafka_consumer_group_committed_offset-alike had wrong keys: {labels.keys()}'

                gens.append(f'GroupOffsetGen()')
                fully_handled = True  # skip standalone label handling
            else:
                raise RuntimeError(
                    f'Unsupported special label set: {mname}{special}')

        if not fully_handled:
            # handle all the standalone labels generically
            for known in sorted(set(labels.keys()).intersection(known_labels)):
                quoted = [f"'{v}'" for v in labels[known]]
                gens.append(f"ListGen('{known}', [{','.join(quoted)}])")

        meta = lookup_meta(mname)

        gen_str = ', '.join(gens)
        lines.append(
            f"    m([{gen_str}], '{mname}', {repr(meta.help)}, '{meta.type}'),\n"
        )

    return lines


def main():

    parser = argparse.ArgumentParser()

    parser.add_argument('metrics',
                        nargs=2,
                        help='Pass metrics and public_metrics input files',
                        type=Path)

    args = parser.parse_args()

    lines = parse_metrics(args.metrics[0], Endpoint.INTERNAL)
    public_lines = parse_metrics(args.metrics[1], Endpoint.PUBLIC)

    print(f"""
METRICS = [
{''.join(lines)}
]

PUBLIC_METRICS = [
{''.join(public_lines)}
]
# yapf: enable

main(METRICS, PUBLIC_METRICS)
""")


main()
