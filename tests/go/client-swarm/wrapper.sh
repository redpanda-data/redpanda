#!/usr/bin/env bash

# wrapper script to set ulimits and friends such that high connection counts can be used
# Run as root: sudo ./wrapper.sh ./client-swarm --brokers foobar --topic foo ...

set -e

# override pids.max cgroup values if running in Kubernetes pod
CGROUP=$(cut -c4- < /proc/self/cgroup)
[[ -f /run/secrets/kubernetes.io/serviceaccount/namespace ]] && while [[ "/" != "${CGROUP}" ]]; do
        [[ $(<"/sys/fs/cgroup/${CGROUP}/pids.max") != "max" ]] && echo "max" > "/sys/fs/cgroup/${CGROUP}/pids.max"
        CGROUP=$(dirname ${CGROUP})
done

# set relevant sysctl values
echo 1 > /proc/sys/net/ipv4/tcp_tw_reuse
echo 15000 64000 > /proc/sys/net/ipv4/ip_local_port_range
echo 1000000 > /proc/sys/vm/max_map_count
echo 1000000 > /proc/sys/kernel/threads-max

# set relevant ulimit values
ulimit -n 1000000
ulimit -s 200000
ulimit -i unlimited
ulimit -u unlimited

# Set GOMAXPROCS if not already set (use all available CPUs)
export GOMAXPROCS=${GOMAXPROCS:-$(nproc)}

$@
