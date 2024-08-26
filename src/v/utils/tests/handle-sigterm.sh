#!/usr/bin/env bash

## Simple bash script that traps SIGTERM

function sigterm_cb() {
  echo -n "sigterm called"
}

trap sigterm_cb SIGTERM

while true; do
  sleep 1
done
