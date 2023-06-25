#!/usr/bin/env bash

TIMEOUT=$1
echo "Run for $TIMEOUT seconds"
DEADLINE=$((SECONDS + TIMEOUT))

while [ $SECONDS -lt $DEADLINE ]; do
  :
done
