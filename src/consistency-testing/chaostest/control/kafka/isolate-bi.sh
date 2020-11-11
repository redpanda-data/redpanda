#!/usr/bin/env bash

set -e

for ip in "$@"; do
  iptables -A INPUT -s $ip -j DROP
  iptables -A OUTPUT -d $ip -j DROP
done
