---
title: Starting a local cluster
order: 0
---
# Starting a local cluster

`rpk container` is a simple and quick way to stand up a local multi node cluster
for testing. `rpk container` leverages Docker. If you haven't done so already,
please follow the installation instructions for
[Docker](https://docs.docker.com/engine/install/) (Linux users) or
[Docker Desktop for Mac](https://www.docker.com/products/docker-desktop) 
(MacOS users).

It's important to note, however, that you won't need to interact with Docker directly or have experience with it.

To get started, run `rpk container start -n 3`. This will start a 3-node cluster. You should see something like this (the addresses may vary):

> `rpk container start` will take a minute the first time you run it, as it will download the latest stable version of Redpanda.

```
Downloading latest version of Redpanda
Starting cluster
  NODE ID  ADDRESS
  0        172.24.1.2:58754
  2        172.24.1.4:58756
  1        172.24.1.3:58757

Cluster started! You may use rpk to interact with it. E.g:

rpk cluster info
```

It says we can check our cluster with `rpk cluster info`

```
  Redpanda Cluster Info

  0 (127.0.0.1:58754)      (No partitions)
  1 (127.0.0.1:58757)      (No partitions)
  2 (127.0.0.1:58756)      (No partitions)
```

You can now connect your Kafka compatible applications directly to Redpanda
by using the ports listed above. In this example the ports to use would be
`58754`, `58757`, `58756`.

Additionally, all of the `rpk topic` subcommands will detect the local cluster and use its addresses, so you don't have to configure anything or keep track of IPs and ports.

For example, you can run `rpk topic create` and it will work!

```
$ rpk topic create -p 6 -r 3 new-topic
Created topic 'new-topic'. Partitions: 6, replicas: 3, cleanup policy: 'delete'
```

You can also stop the cluster using:

```
rpk container stop
```

You are all set! You can now use Redpanda to test your favorite Kafka
compatible application or use the `rpk topic` commands to further interface with
the local cluster!
