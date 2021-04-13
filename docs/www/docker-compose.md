---
title: Docker Compose
---
# Docker compose
## Overview

```yaml

version: '3.7'
services:
  redpanda:
    entrypoint:
    - /usr/bin/rpk
    - redpanda
    - start
    - --smp
    - '1'
    - --reserve-memory
    - 0M
    - --overprovisioned
    - --node-id
    - '0'
    - --kafka-addr
    - PLAINTEXT://0.0.0.0:29092,OUTSIDE://0.0.0.0:9092
    - --advertise-kafka-addr
    - PLAINTEXT://kafka:29092,OUTSIDE://localhost:9092
    - --
    - --kernel-page-cache
    - '1'
    # NOTE: Please use the latest version here!
    image: vectorized/redpanda:v21.4.1
    ports:
    - 9092:9092
    - 29092:29092
    volumes:
    - redpanda:/var/lib/redpanda/data
volumes:
  redpanda: null

```

