from os import path
import os
import sys
import json
import jinja2

PDF_LATENCY = """
set terminal png size 1600,1200
set output "pdf.latency.png"
set multiplot
set boxwidth {{ step }}
has(x) = 1

set xrange [0:{{ xrange }}]

set lmargin 6
set rmargin 10
set tmargin 0

set yrange [0:1]
set size 1, 0.1
set origin 0, 0
set border 11
set xtics nomirror
unset ytics

plot "pdf.latency.log" using 1:(has($2)) notitle with boxes fs solid

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

plot "pdf.latency.log" using 1:2 notitle with boxes

set title "latency (pdf) - {{ title }}"
show title

set parametric
set x2tics ("p99={{ p99 }}" {{ p99 }})
plot [t=0:{{ yrange }}] {{ p99 }},t notitle lt rgb "black"
unset parametric

unset multiplot
"""

AVAILABILITY = """
set terminal png size 1600,1200
set output "availability.png"
set multiplot

set xrange [0:{{ xrange }}]
set yrange [0:{{ yrange }}]

set title "{{ title }}"
show title

set parametric
{% for fault in faults %}plot [t=0:{{ yrange }}] {{ fault }},t notitle lt rgb "red"
{% endfor %}{% for recovery in recoveries %}plot [t=0:{{ yrange }}] {{ recovery }},t notitle lt rgb "blue"
{% endfor %}unset parametric

plot "availability.log" using ($1/1000000):2 title "unavailability (us)" w p ls 7

unset multiplot
"""

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

plot 'overview.lat.log' using 1:2 title "latency" with points lt rgb "black" pt 7

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

plot 'overview.lat.log' using 1:2 title "latency" with points lt rgb "black" pt 7

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


def make_latency_chart(title, log_dir, availability_log, latency_log):
    latencies = []

    with open(path.join(log_dir, latency_log)) as latency_log_file:
        for line in latency_log_file:
            if "ok" in line:
                parts = line.rstrip().split("\t")
                latencies.append(int(parts[1]))

    latencies = sorted(latencies)

    maxlat = latencies[-1]
    p99 = latencies[int(len(latencies) * 0.99)]
    maxfreq = 0
    step = 1000  #us

    with open(path.join(log_dir, "pdf.latency.log"), "w") as latency_file:
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

    with open(path.join(log_dir, "pdf.latency.gp"), "w") as latency_file:
        latency_file.write(
            jinja2.Template(PDF_LATENCY).render(xrange=maxlat,
                                                yrange=1.2 * maxfreq,
                                                p99=p99,
                                                step=step,
                                                title=title))


def make_availability_chart(title, log_dir, availability_log, latency_log):
    maxx = 0
    maxy = 0
    faults = []
    recoveries = []

    with open(path.join(log_dir, "availability.log"),
              "w") as availability_file:
        with open(path.join(log_dir, latency_log)) as latency_log_file:
            last = None
            for line in latency_log_file:
                if "ok" in line:
                    parts = line.rstrip().split("\t")
                    tick = int(parts[0])
                    maxx = max(tick / 1000000, maxx)
                    if last == None:
                        last = int(parts[0])
                        continue
                    delta = tick - last
                    last = tick
                    maxy = max(maxy, delta)
                    availability_file.write(f"{tick}\t{delta}\n")

    maxy = int(1.2 * maxy)

    with open(path.join(log_dir, availability_log)) as availability_log_file:
        for line in availability_log_file:
            entry = json.loads(line)
            tick = int(int(entry["tick"]) / 1000000)
            if entry["type"] == "fault":
                faults.append(tick)
            elif entry["type"] == "recovery":
                recoveries.append(tick)

    with open(path.join(log_dir, "availability.gp"), "w") as availability_file:
        availability_file.write(
            jinja2.Template(AVAILABILITY).render(xrange=maxx,
                                                 yrange=maxy,
                                                 faults=faults,
                                                 recoveries=recoveries,
                                                 title=title))


def make_overview_chart(title, log_dir, availability_log, latency_log):
    maxtick = 0
    maxminlat = 0
    minlatstep = 0
    maxmaxx = 0
    maxlat = 0
    maxthru = 0
    faults = []
    recoveries = []

    with open(path.join(log_dir, "overview.lat.log"), "w") as chart_lat_file:
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

    with open(path.join(log_dir, "overview.1s.log"), "w") as chart_ava_file:
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
