---
title: rpk Container Guide
order: 0
---
# rpk Container Guide

`rpk container` is a simple and quick way to stand up a local multi node cluster
for testing. `rpk container` leverages Docker. If you haven't done so already,
please follow the installation instructions for
[Docker](https://docs.docker.com/engine/install/) (Linux users) or
[Docker Desktop for Mac](https://www.docker.com/products/docker-desktop) 
(MacOS users).

It's important to note, however, that you won't need to interact with Docker directly or have experience with it.

To get started, run `rpk container start -n 3`. This will start a 3-node cluster. You should see something like this (the addresses may vary):

> `rpk container start` will take a minute the first time you run it, since it will download the latest stable version of Redpanda. The next time you run it it should be quicker.

```
Downloading latest version of Redpanda
Starting cluster
  NODE ID  ADDRESS
  0        172.24.1.2:58754
  2        172.24.1.4:58756
  1        172.24.1.3:58757

Cluster started! You may use 'rpk api' to interact with the cluster. E.g:

rpk api status
```

It says we can check our cluster with `rpk api status` Let's try that!

```
  Redpanda Cluster Status

  0 (127.0.0.1:58754)      (No partitions)
  1 (127.0.0.1:58757)      (No partitions)
  2 (127.0.0.1:58756)      (No partitions)
```

You can now connect your Kafka compatible applications directly to Redpanda
by using the ports listed above. In this example the ports to use would be
58754, 58757, 58756.

All of the `rpk api` subcommands will detect the local cluster and use its addresses, so you don't have to configure anything or keep track of IPs and ports.

For example, you can run `rpk api topic create` and it will work!

```
$ rpk api topic create -p 6 -r 3 new-topic
Created topic 'new-topic'. Partitions: 6, replicas: 3, cleanup policy: 'delete'
```

You can also stop the cluster using:

```
rpk container stop
```

You are all set! You can now use Redpanda to test your favorite Kafka
compatiable application or use the `rpk api` commands to further interace with
the local cluster!
