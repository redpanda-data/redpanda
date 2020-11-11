#!/usr/bin/env bash

set -e

for ip in "$@"; do
  iptables -D INPUT -s $ip -j DROP
  iptables -D OUTPUT -d $ip -j DROP
done
