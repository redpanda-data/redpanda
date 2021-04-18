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
    - PLAINTEXT://redpanda:29092,OUTSIDE://localhost:9092
    # NOTE: Please use the latest version here!
    image: vectorized/redpanda:v21.4.12
    ports:
    - 9092:9092
    - 29092:29092

```

