---
title: Support
order: 4
---
# Support

## Feature requests

We love finding new ways to make redpanda better for you. Many of today's
features were proposed by members of our customers' teams.

If you'd like to request a feature or a change, don't hesitate to reach out
through your team's shared Slack channel, and we'll be happy to chat or
schedule a call to discuss your request.

## Critical bugs

If you encounter a problem in a live production cluster that puts its
availability or data safety at risk, please post a message to your team's
shared Slack channel with the following information:

--------------------
- **Environment & Version**

  _Obtained by running `rpk status` in any node in the cluster. If the issue is
  specific to one node, please post the output from said node_. Please include any
  other information you consider relevant.

  If applicable, please also include the cloud vendor and type of machine where
  redpanda is running.

- **Problem description**

  A detailed description of the problem.

- **How to reproduce**

  - Client language:
  - Client library name & version:
  - API version:

  If applicable, please describe the steps to reproduce the problem.

- **Logs**

- Please add any logs as attachments. If you are running redpanda with systemd,
  you can easily retrieve them with `journalctl -n 10000 --no-pager -u redpanda`.

----------------------

The redpanda team has members in Europe, North and South America, so there will
be someone to get the alert and help you troubleshoot.

## Non-critical bugs

For bugs that don't compromise your production cluster's availability or data
safety, please post a message to your team's shared Slack channel using the same
template as above, and we'll create a ticket to triage it and track its fix.

## Common operations

### Updating a node

**Using apt**

```
sudo systemctl stop redpanda
sudo apt update
sudo apt install --only-upgrade redpanda

# alternatively, to update to a specific version
# sudo apt install redpanda=<version>

sudo systemctl start redpanda 
```

**Using yum**

```
sudo systemctl stop redpanda
sudo yum update redpanda

# alternatively, to update to a specific version
# sudo yum update redpanda-<version>

sudo systemctl start redpanda 
```

### Adding a new node

Adding a new node is very easy. You can follow
[the manual installation section](/docs/production-deployment#Manual-Installation)
on setting up a production cluster.

### Changing the config on an existing node

The recommended way to change a node's config is to use `rpk config set`.

For example:

`rpk config set redpanda.log_compaction_interval_ms 600000`

Some configuration fields can't be changed, however.

- `redpanda.node_id`: The node ID identfies a node within a cluster. Changing
  it would be the same as adding a new, completely different cluster.
- `redpanda.seed_nodes`: Changing the list of seed nodes after a node has
  started has no effect, since they're committed to the log.
- `redpanda.{rpc_server,kafka_api,admin}.address`: Right now, changing a node's
  IP isn't supported.
  
### Monitoring

Redpanda exports Prometheus metrics on `<host>:9644/metrics`. To learn how to
quickly generate the necessary configuration, to start scraping it and
visualizing it, see our
[monitoring docs](/docs/monitoring)

## Troubleshooting

### Degraded performance

With redpanda's metrics endpoint and the `rpk generate` configuration for
Prometheus and Grafana, it's easier to monitor and visualize the current cluster
status, as well as to spot sources of service degradation.

These are some metrics to watch out for:
- Tail latencies for the RPC and Kafka APIs
- Error rates

### A node became unavailable

**Verify that the VM is running**

Check that the VM where the redpanda process is hosted is still running. If it
was accidentally stopped, you may start it again and restart redpanda. If you're
using systemd, it's as easy as running `sudo systemctl start redpanda`.
Otherwise, you may use `rpk start` directly.

**Verify that redpanda is running**

If redpanda is still running, check if there are other CPU-intensive processes
running, which may prevent redpanda from making progress. The `redpanda` systemd
service guards against this by requesting a high resource-scheduling priority,
but it might still happen if there are other running services with a higher
still priority or if you're running redpanda without systemd.

Remember that to ensure the best performance possible, redpanda should be the
main process executing in a given machine.

If redpanda stopped, please send a message with the template above. Don't forget
to include the logs and any stack trace. You might also check if there's still
enough available disk space.

If everything looks ok, and redpanda is running, move on to the next step.

**Verify that the node is reachable, and that it can reach the other nodes in
the cluster.**

If you're certain that the redpanda process is running in the affected node, you
can use `telnet` or `ncat` to check that the node is reachable from the rest,
and that it can reach the other nodes. Check your firewall rules to discard any
misconfigurations.

### The 'redpanda' package wasn't found

Make sure that you added our repo to your package manager as explained in our
[Quick Start Linux Guide](/docs/quick-start-linux)

### The 'redpanda' package can't be downloaded

If the `redpanda` package was found, but can't be downloaded, make sure your
rules for outbound traffic haven't changed and allow requests to our repo.

### The node couldn't bootstrap/ start

Check the disk space. If the disk still has space left, please follow the steps
in **Critical bugs** and we'll start looking into it right away.

### The disk is full

Try lowering the `redpanda.compaction_interval_ms` value so that compaction is
performed more often and disk space is freed.

### The disk is corrupted

Start a new node with the same ID as the one whose disk was corrupted. It will
catch up with the current cluster state. If the node that crashed was the leader,
any of the remaining nodes will automatically assume the cluster leadership.

### TLS is failing

Make sure that the installed certificates haven't expired. If they have,
generate new ones and restart the node.
