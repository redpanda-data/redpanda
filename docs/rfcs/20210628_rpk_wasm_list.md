- Feature Name: rpk wasm list
- Status: draft
- Start Date: 2021-06-28
- Authors: Rob Blafford
- Issue:

# Summary

To provide insights into the current state of all coprocessors across the cluster.

Currently the way to dispatch a coprocessor is to use `rpk wasm deploy`. This bundles up the request into a kafka record and writes it into a predefined topic `coprocessor_internal_topic`. Redpanda itself listens for updates on this topic, and starts up the coprocessors that were produced onto the topic. However during and after this process, the only way to know anything about a coprocessor is to look at redpanda or wasm engine logs, or to take a look and see if any coprocessors had created any expected stateful effects.

This proposal increases visibility into the current state of the system by listing all of the available coprocessors, their input topics, and on which nodes they are currently deployed onto.

# Motivation

There is currently no supported way to know about the state of deployed/removed coprocessors within redpanda.

# Guide-level explaination

The way this will be achieved is to have redpanda post all status updates to another predefined topic called `coprocessor_status_topic`. This will be a compacted topic, where the keys for all records will be the node_id of a given redpanda instance.

Each redpanda instance will produce onto this topic once any state change in coprocessors has been detected. The value of the record payload will be in JSON and it will provide information such as registered coprocessors, node_id, and current wasm engine status. A sample JSON payload is provided below:

```
{
    "node_id" : 5,
    "status" : "up",
    "coprocessors" : {
        "name_a" : {
           "input_topics": ["foo", "bar", "baz"],
           "description": "This copro is interesting!"
        },
        "name_b" : {
           "input_topics": ["dd"],
           "description" : "Strips sensitive info from topic"
        }
    }
}
```

With this implementation any tool can consume from the topic, interpret the JSON value and recieve the status. `rpk` does exactly this, and just prints a pretty version of the provided JSON to stdout. If desired the raw JSON can also be printed.

```
  COPROCESSOR NAME  NODE ID  INPUT TOPICS   DESCRIPTION
  name_a            2        [foo,bar,baz]  This copro is interesting!
  name_b            2        [dd]           Strings sensitive info from topic
  name_a            3        [foo,bar,baz]  This copro is interesting!
  name_b            3        [dd]           Strings sensitive info from topic
  name_b            1        [dd]           Strings sensitive info from topic
  name_a            1        [foo,bar,baz]  This copro is interesting!
```

## Drawbacks

Why shouldn't we do this?

There are no drawbacks to providing this feature. In it's implementation the cost of this feature is the cost of maintining another compacted kafka topic (w/ 1 partition, replication factor of 3).
