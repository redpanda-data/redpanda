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

To run Redpanda in a 3-node cluster, run: `rpk container start -n 3`

The first time you run `rpk container start`, it downloads an image matching the rpk version (you can check it by running `rpk version`).
You now have a 3-node cluster running Redpanda!

The command output shows the addresses of the cluster nodes:

```
$ rpk container start -n 3
Starting cluster
Waiting for the cluster to be ready...
  NODE ID  ADDRESS          
  0        127.0.0.1:49462  
  1        127.0.0.1:49468  
  2        127.0.0.1:49467  
```

You can run `rpk` commands to interact with the cluster, for example:

```bash
rpk cluster info --brokers 127.0.0.1:49462,127.0.0.1:49468,127.0.0.1:49467
```

You can now connect your Kafka compatible applications directly to Redpanda
by using the ports listed above. In this example the ports to use would be
`58754`, `58757`, `58756`.

Additionally, all of the `rpk topic` subcommands will detect the local cluster and use its addresses, so you don't have to configure anything or keep track of IPs and ports.

For example, you can run `rpk topic create` and it will work!

```
$ rpk topic create -p 6 -r 3 new-topic --brokers <broker1_address>,<broker2_address>...
Created topic 'new-topic'. Partitions: 6, replicas: 3, cleanup policy: 'delete'
```

You can also stop the cluster using:

```
rpk container stop
```

You are all set! You can now use Redpanda to test your favorite Kafka
compatible application or use the `rpk topic` commands to further interface with
the local cluster!
