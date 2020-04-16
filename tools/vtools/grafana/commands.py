import click
import os
import json
import requests

from ..vlib import config

import grafanalib.core

from absl import logging
from grafanalib.core import (SHORT_FORMAT, single_y_axis, Target, TimeRange,
                             YAxes, YAxis)
import re
from collections import defaultdict


class Metric:
    def __init__(self, name):
        self.name = name


metric_groups = [
    "storage", "reactor", "scheduler", "io_queue",
    "vectorized_internal_rpc_protocol", "kafka_rpc_protocol", "rpc_client",
    "memory"
]


def get_metric_group(name):
    for group in metric_groups:
        if group in name:
            return group
    return "others"


def get_redpanda_metrics(url):

    metrics = defaultdict(list)

    r = requests.get(f'{url}/metrics')

    if r.status_code != 200:
        logging.fatal(f'Unable to get metrics description from {url}')

    done = False
    m = Metric("")
    for line in r.text.splitlines():
        if (line.startswith("# HELP")):
            if done:
                metrics[get_metric_group(m.name)].append(m)
            parts = line.split(" ", 3)
            m = Metric(parts[2].strip())
            m.desc = parts[3].strip()
        elif (line.startswith("# TYPE")):
            parts = line.split(" ", 3)
            m.type = parts[3].strip()
        elif (line.startswith(m.name)):
            labels = []
            labels_str = re.search('\{.+?\}', line).group(0)
            labels_str = labels_str.lstrip("{").rstrip("}")
            for kv in labels_str.split(","):
                parts = kv.split("=")
                lbl = parts[0]
                labels.append(lbl)
                if lbl == 'type':
                    m.subtype = parts[1].strip('"')
            m.labels = labels
            done = True

    return metrics


def legendFormat(m):
    legend = "node: {{instance}}"
    for l in m.labels:
        if l != "type":
            legend += f', {l}: {{{{{l}}}}}'

    return legend


def create_gauge_panel(m, datasource):
    units = grafanalib.core.SHORT_FORMAT
    if "bytes" in m.subtype:
        units = grafanalib.core.BYTES_FORMAT

    g = grafanalib.core.Graph(
        title=m.desc,
        bars=True,
        lines=False,
        dataSource=datasource,
        targets=[
            grafanalib.core.Target(
                expr=
                f'{m.name}{{instance=~"[[node]]",shard=~"[[node_shard]]"}}',
                legendFormat=legendFormat(m),
            ),
        ],
        span=4,
        yAxes=grafanalib.core.YAxes(grafanalib.core.YAxis(format=units)))

    return g


def create_counter_panel(m, datasource):
    units = grafanalib.core.OPS_FORMAT
    if "bytes" in m.subtype:
        units = grafanalib.core.BYTES_PER_SEC_FORMAT

    g = grafanalib.core.Graph(
        title=f'Rate - {m.desc}',
        dataSource=datasource,
        targets=[
            grafanalib.core.Target(
                expr=
                f'irate({m.name}{{instance=~"[[node]]",shard=~"[[node_shard]]"}}[1m])',
                legendFormat=legendFormat(m),
            ),
        ],
        span=4,
        yAxes=grafanalib.core.YAxes(grafanalib.core.YAxis(format=units)))

    return g


def create_dashboard(metrics, datasource):
    nodeTemplate = grafanalib.core.Template(name='node',
                                            dataSource=datasource,
                                            label='Node',
                                            query='label_values(instance)',
                                            multi=True)

    shardTemplate = grafanalib.core.Template(name='node_shard',
                                             dataSource=datasource,
                                             label='Shard',
                                             query='label_values(shard)',
                                             multi=True)
    d = grafanalib.core.Dashboard(
        title="Redpanda",
        rows=[],
        templating=grafanalib.core.Templating(
            list=[nodeTemplate, shardTemplate]),
    )

    for gr, group_metrics in metrics.items():
        row = grafanalib.core.Row(panels=[], showTitle=True, title=gr)
        for m in group_metrics:
            # create rate panel for all counters
            if m.type == "counter":
                row.panels.append(create_counter_panel(m, datasource))
            # create gauge for each metrics
            elif m.subtype != "histogram":
                row.panels.append(create_gauge_panel(m, datasource))
        d.rows.append(row)
    return d.auto_panel_ids()


class DashboardEncoder(json.JSONEncoder):
    def default(self, obj):
        to_json_data = getattr(obj, 'to_json_data', None)
        if to_json_data:
            return to_json_data()
        return json.JSONEncoder.default(self, obj)


def write_dashboard(dashboard, stream):
    json.dump(dashboard.to_json_data(),
              stream,
              sort_keys=True,
              indent=2,
              cls=DashboardEncoder)
    stream.write('\n')


@click.group(short_help='execute grafana related stuff.')
def grafana():
    pass


@grafana.command(short_help='Create redpanda dashboard')
@click.option('--conf',
              help=('Path to configuration file. If not given, a .vtools.yml '
                    'file is searched recursively starting from the current '
                    'working directory'),
              default=None)
@click.option('--url',
              help=('Redpanda metrics endpoint'),
              default="http://localhost:9644")
@click.option('--output', required=False, default=None)
@click.option('--datasource',
              help=('Datasource for redpanda metrics'),
              default="prometheus")
def create(conf, url, output, datasource):
    vconfig = config.VConfig(conf)
    m = get_redpanda_metrics(url)
    d = create_dashboard(m, datasource)
    if output is None:
        output = f"{vconfig.build_root}/grafana_redpanda.json"
    with open(output, "w") as f:
        write_dashboard(d, f)
