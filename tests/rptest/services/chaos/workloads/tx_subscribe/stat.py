# Copyright 2024 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import jinja2
import sys
import traceback
import os
import logging
import subprocess

from ...types import Result
from .log_utils import State, cmds, threads

logger = logging.getLogger("stat")


def gnuplot(path, _cwd=None):
    logger.debug(f"running `gnuplot {path}'...")
    result = subprocess.run(["gnuplot", path],
                            cwd=_cwd,
                            check=True,
                            capture_output=True)
    if len(result.stdout) > 0:
        logger.debug(f"stdout: {result.stdout}")
    if len(result.stderr) > 0:
        logger.debug(f"stderr: {result.stderr}")


def rm_f(path):
    try:
        os.remove(path)
    except FileNotFoundError:
        pass


LATENCY = """
set terminal png size 1600,1200
set output "percentiles.png"
set title "{{ title }}" noenhanced
set multiplot
set yrange [0:{{ yrange }}]
set xrange [-0.1:1.1]

plot "percentiles.log" using 1:2 title "latency (us)" with line lt rgb "black",\\
     {{p99}} title "p99" with lines lt 1

unset multiplot
"""

AVAILABILITY = """
set terminal png size 1600,1200
set output "availability.png"
set title "{{ title }}" noenhanced
show title
plot "availability.log" using ($1/1000):2 title "unavailability (us)" w p ls 7
"""

OVERVIEW = """
set terminal png size 1600,1200
set output "overview.png"
set multiplot
set lmargin 6
set rmargin 10

set pointsize 0.2
set yrange [0:{{ commit_boundary }}]
set xrange [0:{{ duration }}]
set size 1, 0.2
set origin 0, 0

set title "commit time"
show title

set parametric
{% for fault in faults %}plot [t=0:{{ commit_boundary }}] {{ fault }}/1000,t notitle lt rgb "red"
{% endfor %}{% for recovery in recoveries %}plot [t=0:{{ commit_boundary }}] {{ recovery }}/1000,t notitle lt rgb "blue"
{% endfor %}unset parametric

plot 'latency_commit.log' using ($1/1000):2 notitle with points lt rgb "black" pt 7,\\
     {{commit_p99}} title "p99" with lines lt 1

set notitle

set y2range [0:{{ big_latency }}]
set yrange [0:{{ big_latency }}]
set size 1, 0.4
set origin 0, 0.2
unset ytics
set y2tics auto
set tmargin 0
set border 11

set parametric
{% for fault in faults %}plot [t=0:{{ big_latency }}] {{ fault }}/1000,t notitle lt rgb "red"
{% endfor %}{% for recovery in recoveries %}plot [t=0:{{ big_latency }}] {{ recovery }}/1000,t notitle lt rgb "blue"
{% endfor %}unset parametric

plot 'latency_ok.log' using ($1/1000):2 title "latency ok (us)" with points lt rgb "black" pt 7,\\
     'latency_err.log' using ($1/1000):2 title "latency err (us)" with points lt rgb "red" pt 7,\\
     {{p99}} title "p99" with lines lt 1

set title "{{ title }}" noenhanced
show title

set yrange [0:{{ throughput }}]

set size 1, 0.4
set origin 0, 0.6
set format x ""
set bmargin 0
set tmargin 3
set border 15
unset y2tics
set ytics

set parametric
{% for fault in faults %}plot [t=0:{{ throughput }}] {{ fault }}/1000,t notitle lt rgb "red"
{% endfor %}{% for recovery in recoveries %}plot [t=0:{{ throughput }}] {{ recovery }}/1000,t notitle lt rgb "blue"
{% endfor %}unset parametric

plot 'throughput.log' using ($1/1000):2 title "throughput - all (per 1s)" with line lt rgb "black"{% for throughput in throughput_plots %},\\
     '{{throughput.file}}' using ($1/1000):2 title "{{throughput.title}}" with line lt rgb "{{throughput.color}}"{% endfor %}
     

unset multiplot
"""


class ThroughputPlot:
    def __init__(self):
        self.file = None
        self.title = None
        self.color = None


class Throughput:
    def __init__(self):
        self.count = 0
        self.time_ms = 0
        self.history = []

    def tick(self, now_ms):
        while self.time_ms + 1000 < now_ms:
            ts = int(self.time_ms + 1000)
            self.history.append([ts, self.count])
            self.count = 0
            self.time_ms += 1000


class ThroughputBuilder:
    def __init__(self):
        self.total_throughput = None
        self.partition_throughput = dict()

    def build(self, latencies, source_partitions):
        self.total_throughput = Throughput()
        for partition in range(0, source_partitions):
            self.partition_throughput[partition] = Throughput()

        for [ts_ms, latency_us, partition] in latencies:
            self.total_throughput.tick(ts_ms)
            for key in self.partition_throughput.keys():
                self.partition_throughput[key].tick(ts_ms)
            self.total_throughput.count += 1
            self.partition_throughput[partition].count += 1


class LogPlayer:
    def __init__(self):
        self.curr_state = dict()
        self.thread_type = dict()

        self.started_us = None

        self.ts_us = None

        self.latency_err_history = []
        self.latency_ok_history = []
        self.latency_commit_history = []
        self.faults = []
        self.recoveries = []

        self.txn_started = dict()
        self.end_txn_started = dict()
        self.is_commit = dict()
        self.read_partition = dict()

        self.should_measure = False

    def has_at_least(self, num):
        return len(self.latency_commit_history) >= num and len(
            self.latency_ok_history) >= num

    def streaming_apply(self, thread_id, parts):
        if self.curr_state[thread_id] == State.TX:
            self.txn_started[thread_id] = self.ts_us
        elif self.curr_state[thread_id] == State.COMMIT:
            self.is_commit[thread_id] = True
            self.end_txn_started[thread_id] = self.ts_us
        elif self.curr_state[thread_id] == State.ABORT:
            self.is_commit[thread_id] = False
            self.end_txn_started[thread_id] = self.ts_us
        elif self.curr_state[thread_id] == State.READ:
            self.read_partition[thread_id] = int(parts[5])
        elif self.curr_state[thread_id] == State.OK:
            if self.should_measure:
                if self.is_commit[thread_id]:
                    self.latency_ok_history.append([
                        int((self.ts_us - self.started_us) / 1000),
                        self.ts_us - self.txn_started[thread_id],
                        self.read_partition[thread_id]
                    ])
                    self.latency_commit_history.append([
                        int((self.ts_us - self.started_us) / 1000),
                        self.ts_us - self.end_txn_started[thread_id]
                    ])
                else:
                    self.latency_err_history.append([
                        int((self.ts_us - self.started_us) / 1000),
                        self.ts_us - self.txn_started[thread_id]
                    ])

    def apply(self, line):
        parts = line.rstrip().split('\t')

        if parts[2] not in cmds:
            raise Exception(f"unknown cmd \"{parts[2]}\"")

        if self.ts_us == None:
            self.ts_us = int(parts[1])
            self.started_us = self.ts_us
        else:
            delta_us = int(parts[1])
            self.ts_us = self.ts_us + delta_us

        new_state = cmds[parts[2]]

        if new_state == State.EVENT:
            name = parts[3]
            if name == "measure" and not self.should_measure:
                self.started_us = self.ts_us
                self.should_measure = True
            if self.should_measure:
                if name == "injecting" or name == "injected":
                    self.faults.append(
                        int((self.ts_us - self.started_us) / 1000))
                elif name == "healing" or name == "healed":
                    self.recoveries.append(
                        int((self.ts_us - self.started_us) / 1000))
            return
        if new_state == State.VIOLATION:
            return
        if new_state == State.LOG:
            return

        thread_id = int(parts[0])
        if thread_id not in self.curr_state:
            self.thread_type[thread_id] = parts[4]
            self.curr_state[thread_id] = None
            if self.thread_type[thread_id] not in threads:
                raise Exception(f"unknown thread type: {parts[4]}")
        if self.curr_state[thread_id] == None:
            if new_state != State.STARTED:
                raise Exception(
                    f"first logged command of a new thread should be started, got: \"{parts[2]}\""
                )
            self.curr_state[thread_id] = new_state
        else:
            if new_state not in threads[self.thread_type[thread_id]][
                    self.curr_state[thread_id]]:
                raise Exception(
                    f"unknown transition {self.curr_state[thread_id]} -> {new_state}"
                )
            self.curr_state[thread_id] = new_state

        if self.thread_type[thread_id] == "streaming":
            self.streaming_apply(thread_id, parts)


class StatInfo:
    def __init__(self):
        self.latency_err_history = []
        self.latency_ok_history = []
        self.latency_commit_history = []
        self.faults = []
        self.recoveries = []

    def has_at_least(self, num):
        return len(self.latency_commit_history) >= num and len(
            self.latency_ok_history) >= num


def render_overview(title, workload_dir, stat, source_partitions):
    latency_commit_log_path = os.path.join(workload_dir, "latency_commit.log")
    latency_ok_log_path = os.path.join(workload_dir, "latency_ok.log")
    latency_err_log_path = os.path.join(workload_dir, "latency_err.log")

    throughput_log_path = os.path.join(workload_dir, "throughput.log")
    throughput_log_paths = dict()
    for partition in range(0, source_partitions):
        throughput_log_paths[partition] = os.path.join(
            workload_dir, f"throughput_{partition}.log")

    overview_gnuplot_path = os.path.join(workload_dir, "overview.gnuplot")

    try:
        latency_ok = open(latency_ok_log_path, "w")
        latency_err = open(latency_err_log_path, "w")
        latency_commit = open(latency_commit_log_path, "w")
        throughput_log = open(throughput_log_path, "w")
        throughput_logs = dict()
        for key in throughput_log_paths.keys():
            throughput_logs[key] = open(throughput_log_paths[key], "w")

        duration_ms = 0
        min_latency_us = None
        max_latency_us = 0
        commit_p99 = None
        commit_min = None
        commit_max = None
        max_unavailability_us = 0
        max_throughput = 0

        latencies = []
        for [_, latency_us, _] in stat.latency_ok_history:
            latencies.append(latency_us)
        latencies.sort()
        p99 = latencies[int(0.99 * len(latencies))]

        latencies = []
        for [_, latency_us] in stat.latency_commit_history:
            latencies.append(latency_us)
        latencies.sort()
        commit_p99 = latencies[int(0.99 * len(latencies))]
        commit_min = latencies[0]
        commit_max = latencies[-1]

        for [ts_ms, latency_us] in stat.latency_commit_history:
            duration_ms = max(duration_ms, ts_ms)
            latency_commit.write(f"{ts_ms}\t{latency_us}\n")

        max_unavailability_ms = 0
        last_ok = stat.latency_ok_history[0][0]
        for [ts_ms, latency_us, _] in stat.latency_ok_history:
            max_unavailability_ms = max(max_unavailability_ms, ts_ms - last_ok)
            last_ok = ts_ms
            duration_ms = max(duration_ms, ts_ms)
            if min_latency_us == None:
                min_latency_us = latency_us
            min_latency_us = min(min_latency_us, latency_us)
            max_latency_us = max(max_latency_us, latency_us)
            latency_ok.write(f"{ts_ms}\t{latency_us}\n")
        max_unavailability_us = 1000 * max_unavailability_ms

        for [ts_ms, latency_us] in stat.latency_err_history:
            duration_ms = max(duration_ms, ts_ms)
            max_latency_us = max(max_latency_us, latency_us)
            latency_err.write(f"{ts_ms}\t{latency_us}\n")

        throughput_builder = ThroughputBuilder()
        throughput_builder.build(stat.latency_ok_history, source_partitions)

        for [ts_ms, count] in throughput_builder.total_throughput.history:
            duration_ms = max(duration_ms, ts_ms)
            max_throughput = max(max_throughput, count)
            throughput_log.write(f"{ts_ms}\t{count}\n")

        for key in throughput_builder.partition_throughput.keys():
            for [ts_ms, count
                 ] in throughput_builder.partition_throughput[key].history:
                throughput_logs[key].write(f"{ts_ms}\t{count}\n")

        latency_ok.close()
        latency_err.close()
        latency_commit.close()
        throughput_log.close()
        for key in throughput_logs.keys():
            throughput_logs[key].close()

        throughput_plots = []
        # http://phd-bachephysicdun.blogspot.com/2014/01/32-colors-in-gnuplot.html
        colors = ["0x008B8B", "0xB8860B", "0x006400"]
        for key in throughput_builder.partition_throughput.keys():
            if len(
                    list(
                        filter(
                            lambda x: x[1] != 0, throughput_builder.
                            partition_throughput[key].history))) == 0:
                continue
            throughput_plot = ThroughputPlot()
            throughput_plot.file = f"throughput_{key}.log"
            throughput_plot.title = f"throughput partition {key} (per 1s)"
            throughput_plot.color = colors[key]
            throughput_plots.append(throughput_plot)

        with open(overview_gnuplot_path, "w") as gnuplot_file:
            gnuplot_file.write(
                jinja2.Template(OVERVIEW).render(
                    title=title,
                    duration=int(duration_ms / 1000),
                    big_latency=int(p99 * 1.2),
                    p99=p99,
                    commit_p99=commit_p99,
                    commit_boundary=int(commit_p99 * 1.2),
                    faults=stat.faults,
                    recoveries=stat.recoveries,
                    throughput_plots=throughput_plots,
                    throughput=int(max_throughput * 1.2)))

        gnuplot(overview_gnuplot_path, _cwd=workload_dir)
        ops = len(stat.latency_ok_history)

        return {
            "result": Result.PASSED,
            "latency_us": {
                "tx": {
                    "min": min_latency_us,
                    "max": max_latency_us,
                    "p99": p99
                },
                "commit": {
                    "min": commit_min,
                    "max": commit_max,
                    "p99": commit_p99
                }
            },
            "max_unavailability_us": max_unavailability_us,
            "throughput": {
                "avg/s": int(float(1000 * ops) / duration_ms),
                "max/s": max_throughput
            }
        }

    except:
        e, v = sys.exc_info()[:2]
        trace = traceback.format_exc()
        logger.debug(v)
        logger.debug(trace)

        return {"result": Result.UNKNOWN}
    finally:
        rm_f(latency_ok_log_path)
        rm_f(latency_err_log_path)
        rm_f(throughput_log_path)
        for key in throughput_log_paths.keys():
            rm_f(throughput_log_paths[key])
        rm_f(overview_gnuplot_path)
        rm_f(latency_commit_log_path)


def render_availability(title, workload_dir, stat):
    availability_log_path = os.path.join(workload_dir, "availability.log")
    availability_gnuplot_path = os.path.join(workload_dir,
                                             "availability.gnuplot")

    try:
        availability_log = open(availability_log_path, "w")

        last_ok = stat.latency_ok_history[0][0]
        for [ts_ms, latency_us, _] in stat.latency_ok_history:
            availability_log.write(f"{ts_ms}\t{1000 * (ts_ms - last_ok)}\n")
            last_ok = ts_ms

        availability_log.close()

        with open(availability_gnuplot_path, "w") as gnuplot_file:
            gnuplot_file.write(
                jinja2.Template(AVAILABILITY).render(title=title))

        gnuplot(availability_gnuplot_path, _cwd=workload_dir)
    except:
        e, v = sys.exc_info()[:2]
        trace = traceback.format_exc()
        logger.debug(v)
        logger.debug(trace)
    finally:
        rm_f(availability_log_path)
        rm_f(availability_gnuplot_path)


def render_percentiles(title, workload_dir, stat):
    percentiles_log_path = os.path.join(workload_dir, "percentiles.log")
    percentiles_gnuplot_path = os.path.join(workload_dir,
                                            "percentiles.gnuplot")

    try:
        percentiles = open(percentiles_log_path, "w")

        latencies = []
        for [_, latency_us, _] in stat.latency_ok_history:
            latencies.append(latency_us)
        latencies.sort()
        p99 = latencies[int(0.99 * len(latencies))]
        for i in range(0, len(latencies)):
            percentiles.write(f"{float(i) / len(latencies)}\t{latencies[i]}\n")

        percentiles.close()

        with open(percentiles_gnuplot_path, "w") as latency_file:
            latency_file.write(
                jinja2.Template(LATENCY).render(title=title,
                                                yrange=int(p99 * 1.2),
                                                p99=p99))

        gnuplot(percentiles_gnuplot_path, _cwd=workload_dir)
    except:
        e, v = sys.exc_info()[:2]
        trace = traceback.format_exc()
        logger.debug(v)
        logger.debug(trace)
    finally:
        rm_f(percentiles_log_path)
        rm_f(percentiles_gnuplot_path)


def collect(title, workload_dir, workload_nodes, source_partitions):
    logger.setLevel(logging.DEBUG)
    logger_handler_path = os.path.join(workload_dir, "stat.log")
    handler = logging.FileHandler(logger_handler_path)
    handler.setFormatter(
        logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
    logger.addHandler(handler)

    ret = {"result": Result.PASSED}
    total = StatInfo()
    for node in workload_nodes:
        node_dir = f"{workload_dir}/{node}"
        if os.path.isdir(node_dir):
            player = LogPlayer()

            with open(os.path.join(node_dir, "workload.log"),
                      "r") as workload_file:
                last_line = None
                for line in workload_file:
                    if last_line != None:
                        player.apply(last_line)
                    last_line = line

            total.latency_err_history.extend(player.latency_err_history)
            total.latency_ok_history.extend(player.latency_ok_history)
            total.latency_commit_history.extend(player.latency_commit_history)
            total.faults = player.faults
            total.recoveries = player.recoveries

            if player.has_at_least(10):
                node_result = render_overview(title, node_dir, player,
                                              source_partitions)
                logger.info(f"node {node} stats: {node_result}")
                render_availability(title, node_dir, player)
                render_percentiles(title, node_dir, player)
            else:
                node_result = {"result": Result.NODATA}
                logger.info(f"not enough data to calculate node {node} stats")
        else:
            node_result = {
                "result": Result.UNKNOWN,
            }
            logger.info(f"can't find logs for node {node}")

        ret[node] = node_result
        ret["result"] = Result.more_severe(ret["result"],
                                           node_result["result"])

    if total.has_at_least(10):
        total.latency_err_history.sort(key=lambda x: x[0])
        total.latency_ok_history.sort(key=lambda x: x[0])
        total.latency_commit_history.sort(key=lambda x: x[0])

        total_result = render_overview(title, workload_dir, total,
                                       source_partitions)
        logger.info(f"total stats: {total_result}")
    else:
        total_result = {"result": Result.NODATA}
        logger.info(f"not enough data to calculate total stats")

    ret["total"] = total_result
    ret["result"] = Result.more_severe(ret["result"], total_result["result"])

    handler.flush()
    handler.close()
    logger.removeHandler(handler)

    return ret
