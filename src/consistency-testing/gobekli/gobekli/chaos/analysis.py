from os import path
import os
import sys
import json
import jinja2

OVERVIEW = """
set terminal png size 1600,1200
set output "overview.png"
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

plot 'chart.lat.log' using 1:2 title "latency" with points lt rgb "black" pt 7

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

plot 'chart.lat.log' using 1:2 title "latency" with points lt rgb "black" pt 7

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

plot 'chart.1s.log' using 1:2 title "ops per 1s" with line lt rgb "black"

unset multiplot
"""


def analyze_inject_recover_availability(log_dir, availability_log,
                                        latency_log):
    maxlat = 0
    minlat = sys.maxsize

    first_fault = sys.maxsize
    last_recovery = 0

    with open(path.join(log_dir, availability_log)) as availability_log_file:
        line = availability_log_file.readline()
        while line:
            entry = json.loads(line)
            tick = entry["tick"]
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

    with open(path.join(log_dir, latency_log)) as latency_log_file:
        line = latency_log_file.readline()
        while line:
            if "ok" in line:
                parts = line.rstrip().split("\t")
                tick = int(parts[0])
                lat = int(parts[1])
                maxlat = max(maxlat, lat)
                minlat = min(minlat, lat)

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

            line = latency_log_file.readline()

    return {
        "max_lat": maxlat,
        "min_lat": minlat,
        "max_unavailability": maxunava,
        "min_unavailability": minunava,
        "base_max_unavailability": maxunava_base,
        "fault_max_unavailability": maxunava_fault,
        "recovery_max_unavailability": maxunava_recovery,
    }


def make_overview_chart(title, log_dir, availability_log, latency_log):
    maxtick = 0
    maxminlat = 0
    minlatstep = 0
    maxmaxx = 0
    maxlat = 0
    maxthru = 0
    faults = []
    recoveries = []

    with open(path.join(log_dir, "chart.lat.log"), "w") as chart_lat_file:
        with open(path.join(log_dir, latency_log)) as latency_log_file:
            mn = sys.maxsize
            for line in latency_log_file:
                if "ok" in line:
                    parts = line.rstrip().split("\t")
                    tick = int(int(parts[0]) / 1000000)
                    maxtick = max(maxtick, tick)
                    lat = int(parts[1])
                    out = f"{tick}\t{lat}"
                    for i in range(3, len(parts)):
                        out += "\t" + parts[i]
                    chart_lat_file.write(out + "\n")
                    mn = min(mn, lat)
                    maxlat = max(maxlat, lat)
            maxminlat = mn * 3
            minlatstep = mn
            maxmaxx = int(1.2 * maxlat)

    with open(path.join(log_dir, "chart.1s.log"), "w") as chart_ava_file:
        with open(path.join(log_dir,
                            availability_log)) as availability_log_file:
            should_skip = True
            mx_thru = 0

            for line in availability_log_file:
                entry = json.loads(line)
                tick = entry["tick"]
                if entry["type"] == "stat":
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
                    faults.append(int(tick / 1000000))
                elif entry["type"] == "recovery":
                    recoveries.append(int(tick / 1000000))

            maxthru = int(1.2 * mx_thru)

    with open(path.join(log_dir, "overview.gp"), "w") as overview_file:
        overview_file.write(
            jinja2.Template(OVERVIEW).render(xrange=maxtick,
                                             maxminlat=maxminlat,
                                             minlatstep=minlatstep,
                                             maxmaxx=maxmaxx,
                                             maxlat=maxlat,
                                             maxthru=maxthru,
                                             faults=faults,
                                             recoveries=recoveries,
                                             title=title))
