# Copyright 2020 Vectorized, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

from os import path
from collections import defaultdict
import os
from os import path
import sys
import json
from enum import Enum
import jinja2
from pathlib import Path

PDF_LATENCY = """
set terminal png size 1600,1200
set output "pdf.latency.{{ latency_type_name }}.{{ endpoint_idx }}.png"
set multiplot
set boxwidth {{ step }}
has(x) = 1

set xrange [0:{{ xzrange }}]

set lmargin 6
set rmargin 10
set tmargin 0

set yrange [0:1]
set size 1, 0.1
set origin 0, 0
set border 11
set xtics nomirror
unset ytics

plot "pdf.latency.{{ latency_type_name }}.{{ endpoint_idx }}.log" using 1:(has($2)) notitle with boxes fs solid

set parametric
plot [t=0:1] {{ p99 }},t notitle lt rgb "black"
unset parametric

set tmargin 5
set bmargin 0

set yrange [0:{{ yrange }}]
set size 1, 0.9
set origin 0, 0.1
set border 15
unset xtics
set ytics auto

plot "pdf.latency.{{ latency_type_name }}.{{ endpoint_idx }}.log" using 1:2 notitle with boxes

set title "[{{ endpoint_idx }}] latency (pdf) - {{ title }}"
show title

set parametric
set x2tics ("p99={{ p99 }}" {{ p99 }})
plot [t=0:{{ yrange }}] {{ p99 }},t notitle lt rgb "black"
unset parametric

################################################################################

set xrange [0:{{ xrange }}]
unset title
unset x2tics

set tmargin 0
set yrange [0:1]
set size 0.4, 0.04
set origin 0.5, 0.56
set border 11
set xtics ("{{ p99 }}" {{ p99 }}, "{{ xrange }}" {{ xrange }})
unset ytics

set object 1 rectangle from graph 0, graph 0 to graph 1, graph 1 behind fillcolor rgb 'white' fillstyle solid noborder
plot "pdf.latency.{{ latency_type_name }}.{{ endpoint_idx }}.log" using 1:(has($2)) notitle with boxes fs solid
unset object 1
set parametric
plot [t=0:1] {{ p99 }},t notitle lt rgb "black"
unset parametric

set yrange [0:{{ yrange }}]
set size 0.4, 0.35
set origin 0.5, 0.58
set border 15
set tmargin 5
set bmargin 0
set ytics auto
unset xtics

set object 2 rectangle from graph 0, graph 0 to graph 1, graph 1 behind fillcolor rgb 'white' fillstyle solid noborder
plot "pdf.latency.{{ latency_type_name }}.{{ endpoint_idx }}.log" using 1:2 notitle with boxes
unset object 2
set parametric
plot [t=0:{{ yrange }}] {{ p99 }},t notitle lt rgb "black"
unset parametric

################################################################################

unset multiplot
"""

AVAILABILITY = """
set terminal png size 1600,1200
set output "availability.{{ endpoint_idx }}.png"
set multiplot

set xrange [0:{{ xrange }}]
set yrange [0:{{ yrange }}]

set title "{{ title }}"
show title

set parametric
{% for fault in faults %}plot [t=0:{{ yrange }}] {{ fault }},t notitle lt rgb "red"
{% endfor %}{% for recovery in recoveries %}plot [t=0:{{ yrange }}] {{ recovery }},t notitle lt rgb "blue"
{% endfor %}unset parametric

plot "availability.{{ endpoint_idx }}.log" using ($1/1000000):2 title "unavailability (us)" w p ls 7

unset multiplot
"""

OVERVIEW = """
set terminal png size 1600,1200
set output "overview.{{ latency_type_name }}.png"
set multiplot

set lmargin 6
set rmargin 10

set xrange [0:{{ xrange }}]

set yrange [0:{{ maxminlat }}]
set pointsize 0.2

set size 1, 0.2
set origin 0, 0
unset ytics
set y2tics {{ minlatstep }}

set parametric
{% for fault in faults %}plot [t=0:{{ maxminlat }}] {{ fault }},t notitle lt rgb "red"
{% endfor %}{% for recovery in recoveries %}plot [t=0:{{ maxminlat }}] {{ recovery }},t notitle lt rgb "blue"
{% endfor %}unset parametric

plot 'overview.lat.{{ latency_type_name }}.log' using 1:(strcol(3) eq 'ok' ? $2:1/0) title "latency ok (us)" with points lt rgb "black" pt 7,\
     'overview.lat.{{ latency_type_name }}.log' using 1:(strcol(3) eq 'err' ? $2:1/0) title "latency err (us)" with points lt rgb "red" pt 7,\
     'overview.lat.{{ latency_type_name }}.log' using 1:(strcol(3) eq 'out' ? $2:1/0) title "latency timeout (us)" with points lt rgb "blue" pt 7

set yrange [0:{{ maxmaxx }}]
set pointsize 0.2

set size 1, 0.4
set origin 0, 0.2
unset ytics
set y2tics auto
set tmargin 0
set border 11

plot {{ maxlat }} notitle with lines lt rgb "red"

set parametric
{% for fault in faults %}plot [t=0:{{ maxmaxx }}] {{ fault }},t notitle lt rgb "red"
{% endfor %}{% for recovery in recoveries %}plot [t=0:{{ maxmaxx }}] {{ recovery }},t notitle lt rgb "blue"
{% endfor %}unset parametric

plot 'overview.lat.{{ latency_type_name }}.log' using 1:(strcol(3) eq 'ok' ? $2:1/0) title "latency ok (us)" with points lt rgb "black" pt 7,\
     'overview.lat.{{ latency_type_name }}.log' using 1:(strcol(3) eq 'err' ? $2:1/0) title "latency err (us)" with points lt rgb "red" pt 7,\
     'overview.lat.{{ latency_type_name }}.log' using 1:(strcol(3) eq 'out' ? $2:1/0) title "latency timeout (us)" with points lt rgb "blue" pt 7

set title "{{ title }}"
show title

set yrange [0:{{ maxthru }}]

set size 1, 0.4
set origin 0, 0.6
set format x ""
set bmargin 0
set tmargin 3
set border 15
unset y2tics
set ytics

set parametric
{% for fault in faults %}plot [t=0:{{ maxthru }}] {{ fault }},t notitle lt rgb "red"
{% endfor %}{% for recovery in recoveries %}plot [t=0:{{ maxthru }}] {{ recovery }},t notitle lt rgb "blue"
{% endfor %}unset parametric

plot 'overview.1s.log' using 1:2 title "ops per 1s" with line lt rgb "black"

unset multiplot
"""

LATENCY = """
set terminal png size 1600,1200
set output "latency.{{ latency_type_name }}.{{ endpoint_idx }}.png"
set multiplot

set lmargin 6
set rmargin 10

set xrange [0:{{ xrange }}]

set yrange [0:{{ maxminlat }}]
set pointsize 0.2

set size 1, 0.3
set origin 0, 0
unset ytics
set y2tics {{ minlatstep }}

set parametric
{% for fault in faults %}plot [t=0:{{ maxminlat }}] {{ fault }},t notitle lt rgb "red"
{% endfor %}{% for recovery in recoveries %}plot [t=0:{{ maxminlat }}] {{ recovery }},t notitle lt rgb "blue"
{% endfor %}unset parametric

plot 'latency.{{ latency_type_name }}.{{ endpoint_idx }}.log' using 1:(strcol(3) eq 'ok' ? $2:1/0) title "latency ok (us)" with points lt rgb "black" pt 7,\
     'latency.{{ latency_type_name }}.{{ endpoint_idx }}.log' using 1:(strcol(3) eq 'err' ? $2:1/0) title "latency err (us)" with points lt rgb "red" pt 7,\
     'latency.{{ latency_type_name }}.{{ endpoint_idx }}.log' using 1:(strcol(3) eq 'out' ? $2:1/0) title "latency timeout (us)" with points lt rgb "blue" pt 7

set title "{{ title }}"
show title

set yrange [0:{{ maxmaxx }}]
set pointsize 0.2

set size 1, 0.65
set origin 0, 0.3
unset ytics
set y2tics auto
set tmargin 0

plot {{ maxlat }} notitle with lines lt rgb "red"

set parametric
{% for fault in faults %}plot [t=0:{{ maxmaxx }}] {{ fault }},t notitle lt rgb "red"
{% endfor %}{% for recovery in recoveries %}plot [t=0:{{ maxmaxx }}] {{ recovery }},t notitle lt rgb "blue"
{% endfor %}unset parametric

plot 'latency.{{ latency_type_name }}.{{ endpoint_idx }}.log' using 1:(strcol(3) eq 'ok' ? $2:1/0) title "latency ok (us)" with points lt rgb "black" pt 7,\
     'latency.{{ latency_type_name }}.{{ endpoint_idx }}.log' using 1:(strcol(3) eq 'err' ? $2:1/0) title "latency err (us)" with points lt rgb "red" pt 7,\
     'latency.{{ latency_type_name }}.{{ endpoint_idx }}.log' using 1:(strcol(3) eq 'out' ? $2:1/0) title "latency timeout (us)" with points lt rgb "blue" pt 7

unset multiplot
"""


def analyze_inject_recover_availability(log_dir,
                                        availability_log,
                                        latency_log,
                                        warmup_s=2):
    availability_cut_off_s = warmup_s
    latency_cut_off_us = warmup_s * 1000000
    maxlat = 0
    minlat = sys.maxsize

    first_fault = sys.maxsize
    last_recovery = 0

    with open(path.join(log_dir, availability_log)) as availability_log_file:
        line = availability_log_file.readline()
        while line:
            entry = json.loads(line)
            tick = entry["tick"]
            if tick >= availability_cut_off_s:
                if entry["type"] == "fault":
                    first_fault = min(int(tick), first_fault)
                elif entry["type"] == "recovery":
                    last_recovery = max(int(tick), last_recovery)
            line = availability_log_file.readline()

    last_ok = None
    maxunava = 0
    minunava = sys.maxsize
    maxunava_recovery = 0
    maxunava_fault = 0
    maxunava_base = 0
    latencies = []

    with open(path.join(log_dir, latency_log)) as latency_log_file:
        for line in latency_log_file:
            if "ok" in line:
                parts = line.rstrip().split("\t")
                tick = int(parts[0])
                if tick < latency_cut_off_us:
                    continue
                lat = int(parts[1])
                maxlat = max(maxlat, lat)
                minlat = min(minlat, lat)
                latencies.append(lat)

                if last_ok == None:
                    last_ok = tick
                else:
                    unava = sys.maxsize

                    if tick < first_fault:
                        unava = tick - last_ok
                        maxunava_base = max(maxunava_base, unava)
                    elif tick < last_recovery:
                        if last_ok < first_fault:
                            unava = tick - first_fault
                        else:
                            unava = tick - last_ok
                        maxunava_fault = max(maxunava_fault, unava)
                    else:
                        if last_ok < last_recovery:
                            unava = tick - last_recovery
                            if first_fault < last_ok:
                                maxunava_fault = max(maxunava_fault,
                                                     last_recovery - last_ok)
                            else:
                                maxunava_fault = max(
                                    maxunava_fault,
                                    last_recovery - first_fault)
                        else:
                            unava = tick - last_ok
                        maxunava_recovery = max(maxunava_recovery, unava)
                    maxunava = max(maxunava, tick - last_ok)
                    minunava = min(minunava, tick - last_ok)
                    last_ok = tick

    latencies.sort()

    return {
        "max_lat": maxlat,
        "min_lat": minlat,
        "p99_lat": latencies[int(0.99 * len(latencies))],
        "second_max_lat": latencies[-2],
        "max_unavailability": maxunava,
        "min_unavailability": minunava,
        "base_max_unavailability": maxunava_base,
        "fault_max_unavailability": maxunava_fault,
        "recovery_max_unavailability": maxunava_recovery,
    }


class ExperimentGroup:
    def __init__(self, workload, scenario, fault):
        self.scenario = scenario
        self.workload = workload
        self.fault = fault
        self.experiments = []


class LatencyType(Enum):
    OVERALL = 1
    PRODUCER = 4


def make_pdf_latency_chart(title, endpoint_idx, log_dir, availability_log,
                           latency_log, warmup_s, zoom_range_us, latency_type):
    latency_cut_off_us = warmup_s * 1000000
    latencies = []

    with open(path.join(log_dir, latency_log)) as latency_log_file:
        for line in latency_log_file:
            if "ok" in line:
                parts = line.rstrip().split("\t")
                tick = int(parts[0])
                latency = int(parts[latency_type.value])
                idx = int(parts[3])
                if endpoint_idx != None:
                    if endpoint_idx != idx:
                        continue
                if tick < latency_cut_off_us:
                    continue
                latencies.append(latency)

    latencies = sorted(latencies)

    maxlat = latencies[-1]
    p99 = latencies[int(len(latencies) * 0.99)]
    maxfreq = 0
    step = 1000  #us

    if endpoint_idx == None:
        endpoint_idx = "all"

    latency_type_name = latency_type.name.lower()

    with open(
            path.join(log_dir,
                      f"pdf.latency.{latency_type_name}.{endpoint_idx}.log"),
            "w") as latency_file:
        offset = 0
        freq = []

        i = 0
        mark = offset
        while True:
            c = 0
            while i < len(latencies) and latencies[i] < mark + step:
                i += 1
                c += 1
            freq.append(c)
            mark += step
            if i >= len(latencies):
                break
        #
        area = len(list(filter(lambda x: x > 0, freq)))
        s = sum(freq)

        if 100 < area:
            step = int((area * step) / 100)

        i = 0
        mark = offset
        while True:
            c = 0
            while i < len(latencies) and latencies[i] < mark + step:
                i += 1
                c += 1
            c = float(c) / s
            maxfreq = max(maxfreq, c)
            if c > 0:
                latency_file.write(f"{mark}\t{c}\n")
            mark += step
            if i >= len(latencies):
                break

    with open(
            path.join(log_dir,
                      f"pdf.latency.{latency_type_name}.{endpoint_idx}.gp"),
            "w") as latency_file:
        latency_file.write(
            jinja2.Template(PDF_LATENCY).render(
                xrange=maxlat,
                xzrange=min(maxlat, zoom_range_us),
                yrange=1.2 * maxfreq,
                p99=p99,
                latency_type_name=latency_type_name,
                step=step,
                title=title,
                endpoint_idx=endpoint_idx))


def make_availability_chart(title, endpoint_idx, log_dir, availability_log,
                            latency_log, warmup_s):
    latency_cut_off_us = warmup_s * 1000000
    availability_cut_off_s = warmup_s

    maxx = 0
    maxy = 0

    if endpoint_idx == None:
        ava_log = "availability.all.log"
    else:
        ava_log = f"availability.{endpoint_idx}.log"

    with open(path.join(log_dir, ava_log), "w") as availability_file:
        with open(path.join(log_dir, latency_log)) as latency_log_file:
            last = None
            for line in latency_log_file:
                if "ok" in line:
                    parts = line.rstrip().split("\t")
                    tick = int(parts[0])
                    idx = int(parts[3])
                    if tick < latency_cut_off_us:
                        continue
                    maxx = max(tick / 1000000, maxx)
                    if last == None:
                        last = int(parts[0])
                        continue
                    if endpoint_idx != None:
                        if endpoint_idx != idx:
                            continue
                    delta = tick - last
                    last = tick
                    maxy = max(maxy, delta)
                    availability_file.write(f"{tick}\t{delta}\n")

    maxy = int(1.2 * maxy)
    faults = []
    recoveries = []

    with open(path.join(log_dir, availability_log)) as availability_log_file:
        for line in availability_log_file:
            entry = json.loads(line)
            tick = int(int(entry["tick"]) / 1000000)
            if tick >= availability_cut_off_s:
                if entry["type"] == "fault":
                    faults.append(tick)
                elif entry["type"] == "recovery":
                    recoveries.append(tick)

    if endpoint_idx == None:
        endpoint_idx = "all"

    with open(path.join(log_dir, f"availability.{endpoint_idx}.gp"),
              "w") as availability_file:
        availability_file.write(
            jinja2.Template(AVAILABILITY).render(xrange=maxx,
                                                 yrange=maxy,
                                                 faults=faults,
                                                 recoveries=recoveries,
                                                 title=title,
                                                 endpoint_idx=endpoint_idx))


def make_overview_chart(title, log_dir, availability_log, latency_log,
                        warmup_s, latency_type):
    latency_cut_off_us = warmup_s * 1000000
    availability_cut_off_s = warmup_s

    maxtick = 0
    maxminlat = 0
    minlatstep = 0
    maxmaxx = 0
    maxlat = 0
    maxthru = 0
    faults = []
    recoveries = []

    latency_type_name = latency_type.name.lower()

    with open(path.join(log_dir, f"overview.lat.{latency_type_name}.log"),
              "w") as chart_lat_file:
        with open(path.join(log_dir, latency_log)) as latency_log_file:
            mn = sys.maxsize
            for line in latency_log_file:
                if latency_type == LatencyType.PRODUCER:
                    if "ok" not in line:
                        continue
                parts = line.rstrip().split("\t")
                tick = int(parts[0])
                if tick < latency_cut_off_us:
                    continue
                tick = int(tick / 1000000)
                lat = int(parts[latency_type.value])
                if "ok" in line:
                    maxtick = max(maxtick, tick)
                    mn = min(mn, lat)
                    maxlat = max(maxlat, lat)
                out = f"{tick}\t{lat}"
                for i in range(2, len(parts)):
                    out += "\t" + parts[i]
                chart_lat_file.write(out + "\n")
            maxminlat = mn * 3
            minlatstep = mn
            maxmaxx = int(1.2 * maxlat)

    with open(path.join(log_dir, "overview.1s.log"), "w") as chart_ava_file:
        with open(path.join(log_dir,
                            availability_log)) as availability_log_file:
            should_skip = True
            mx_thru = 0

            for line in availability_log_file:
                entry = json.loads(line)
                tick = entry["tick"]
                if entry["type"] == "stat":
                    if tick >= availability_cut_off_s:
                        thru = entry["all:ok"]
                        maxtick = max(maxtick, tick)
                        mx_thru = max(thru, mx_thru)
                        if should_skip:
                            if thru != 0:
                                should_skip = False
                        if not (should_skip):
                            chart_ava_file.write(
                                str(tick) + "\t" + str(thru) + "\n")
                elif entry["type"] == "fault":
                    tick = int(tick / 1000000)
                    if tick >= availability_cut_off_s:
                        faults.append(tick)
                elif entry["type"] == "recovery":
                    tick = int(tick / 1000000)
                    if tick >= availability_cut_off_s:
                        recoveries.append(tick)

            maxthru = int(1.2 * mx_thru)

    with open(path.join(log_dir, f"overview.{latency_type_name}.gp"),
              "w") as overview_file:
        overview_file.write(
            jinja2.Template(OVERVIEW).render(
                xrange=maxtick,
                maxminlat=maxminlat,
                minlatstep=minlatstep,
                maxmaxx=maxmaxx,
                maxlat=maxlat,
                maxthru=maxthru,
                latency_type_name=latency_type_name,
                faults=faults,
                recoveries=recoveries,
                title=title))


def make_latency_chart(title, endpoint_idx, log_dir, availability_log,
                       latency_log, warmup_s, latency_type):
    latency_cut_off_us = warmup_s * 1000000
    availability_cut_off_s = warmup_s

    maxtick = 0
    maxminlat = 0
    minlatstep = 0
    maxmaxx = 0
    maxlat = 0
    maxthru = 0

    latency_type_name = latency_type.name.lower()

    with open(
            path.join(log_dir,
                      f"latency.{latency_type_name}.{endpoint_idx}.log"),
            "w") as chart_lat_file:
        with open(path.join(log_dir, latency_log)) as latency_log_file:
            mn = sys.maxsize
            for line in latency_log_file:
                if latency_type == LatencyType.PRODUCER:
                    if "ok" not in line:
                        continue
                parts = line.rstrip().split("\t")
                tick = int(parts[0])
                if tick < latency_cut_off_us:
                    continue
                tick = int(tick / 1000000)
                lat = int(parts[latency_type.value])
                idx = int(parts[3])
                if idx != endpoint_idx:
                    continue
                maxmaxx = max(maxmaxx, lat)
                maxtick = max(maxtick, tick)
                if "ok" in line:
                    mn = min(mn, lat)
                    maxlat = max(maxlat, lat)
                out = f"{tick}\t{lat}"
                out += "\t" + parts[2]
                chart_lat_file.write(out + "\n")
            maxminlat = mn * 3
            minlatstep = mn
            maxmaxx = int(1.2 * maxmaxx)

    faults = []
    recoveries = []

    with open(path.join(log_dir, availability_log)) as availability_log_file:
        for line in availability_log_file:
            entry = json.loads(line)
            tick = int(int(entry["tick"]) / 1000000)
            if tick >= availability_cut_off_s:
                if entry["type"] == "fault":
                    faults.append(tick)
                elif entry["type"] == "recovery":
                    recoveries.append(tick)

    with open(
            path.join(log_dir,
                      f"latency.{latency_type_name}.{endpoint_idx}.gp"),
            "w") as overview_file:
        overview_file.write(
            jinja2.Template(LATENCY).render(
                xrange=maxtick,
                maxminlat=maxminlat,
                minlatstep=minlatstep,
                maxmaxx=maxmaxx,
                maxlat=maxlat,
                maxthru=maxthru,
                faults=faults,
                latency_type_name=latency_type_name,
                recoveries=recoveries,
                title=title,
                endpoint_idx=endpoint_idx))
