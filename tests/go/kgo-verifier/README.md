
# kgo-verifier & kgo-repeater

These are Kafka traffic generators using franz-go for driving significant
throughput on a Redpanda cluster.

## kgo-repeater

### Purpose

Scalable (across multiple nodes) population of producers and consumers.  Each
worker is a member of a consumer group, and echos consumed messages back to
the cluster as a producer.  This scheme provides straightforward scaling,
as starting additional workers automatically re-distributes production
work alongside the consumption work.

kgo-repeater does not validate the messages it receives, beyond that they
should be syntactically valid and contain the fields it expects: for validation
that messages arrive in the right order and are not lost, use kgo-verifier.

kgo-repeater also provides basic latency measurement, although it is not
engineered to be a rigorous benchmark.

### Examples

#### Single standalone process

    kgo-repeater --brokers 127.0.0.1:9092 -group mygroup -workers 4 -topic mytopic

That will run 4 clients, each consuming and producing.  A
default 4MB of data will be injected into the topic on
startup, and then repeated back through the clients
until the process is signalled to stop.

#### Group of processes

While you can run multiple independent kgo-repeater processes using different
consumer groups, to have multiple processes share the same consumer group (
thereby share work neatly across the clients and exercise larger consumer groups),
there is an HTTP interface to remotely control a group of processes.

The main reason for this remote control is to coordinate startup: ensuring that
the data is not injected into the topic under test until the workers have all
come up and joined the consumer group (otherwise data might be lost and we can
end up driving less data through the system than we intended to)

For example, start two processes with the `-remote` parameter:
    kgo-repeater --brokers 127.0.0.1:9092 -group mygroup -workers 4 -topic mytopic -remote -remote-port 7884
    kgo-repeater --brokers 127.0.0.1:9092 -group mygroup -workers 4 -topic mytopic -remote -remote-port 7885

These processes will start their consumers, but not produce anything until
requested to via an HTTP operation.  To start the producers:

    curl -X PUT localhost:7884/activate
    curl -X PUT localhost:7885/activate

You can then query the progress of the processes:

    # The result is a vector of dicts, one per worker
    # Latencies are in microseconds.
    curl -X get localhost:7884/status
    [
    {
        "produced": 6719,
        "consumed": 6489,
        "enqueued": 25,
        "errors": 0,
        "latency": {
        "ack": {
            "p50": 5128.5,
            "p90": 6379.5,
            "p99": 12562.5
        },


## kgo-verifier

### Purpose

This is a test utility for validating Redpanda data integrity under
various produce/consume patterns, especially random reads that stress
tiered storage.

Stress test redpanda with concurrent random reads, a particularly important
case for validating tiered storage (shadow indexing) where random reads
tend to lead to lots of cache promotion/demotion.

The tool is meant to be chaos-tolerant, i.e. it should not drop out when brokers become
unavailable or cannot respond to requests, and it should continue to be able to validate
its own output if that output was written in bad conditions (e.g. with producer retries).

Additionally verify content of reads, to check for offset translation issues:
the key of each message includes the offset where the producer expects it to land.
The producer keeps track of which messages were successfully committed at the expected
offset, and writes it out to a file.  Consumers then consult this file while reading,
to check whether a particular offset is expected to contain a valid message (i.e. key
matches offset) or not.

### Usage

- Use of TLS is allowed (through `--enable-tls`) with the caveat that the certificate
  must be signed by a known/trusted CA (so no self-signed or self generated CAs)

#### 1. Quick produce+consume smoke test: produce and then consume in the same process

    kgo-verifier --brokers $BROKERS --username $SASL_USER --password $SASL_PASSWORD --topic $TOPIC --msg_size 128000 --produce_msgs 10000 --rand_read_msgs 10 --seq_read=1


#### 2. A long running producer

Run exactly one of these at a time, it writes out
a valid_offsets_{topic}.json file, so multiple concurrent producers would 
interfere with one another

    kgo-verifier --brokers $BROKERS --username $SASL_USER --password $SASL_PASSWORD --topic $TOPIC --msg_size 128000 --produce_msgs 10000 --rand_read_msgs 0 --seq_read=0

#### 3. A sequential consumer.

Run one of these inside a while loop to continuously stream
the whole content of the topic.

    kgo-verifier --brokers $BROKERS --username $SASL_USER --password $SASL_PASSWORD --topic $TOPIC --msg_size 128000 --produce_msgs 0 --rand_read_msgs 0 --seq_read=1 


#### 4. A parallel random consumer
The --parallel flag says how many read fibers to run concurently

    kgo-verifier --brokers $BROKERS --username $SASL_USER --password $SASL_PASSWORD --topic $TOPIC --msg_size 128000 --produce_msgs 0 --rand_read_msgs 10 --seq_read=0 --parallel 4

#### 5. A *very* parallel random consumer
aims to emit so many concurrent reads
that the shadow index cache may violate its size bounds (e.g. do 64 concurrent
reads of 1GB segments, when the cache size limit is only 50GB).
Keep rand_read_msgs at 1 to constrain memory usage.

    kgo-verifier --brokers $BROKERS --username $SASL_USER --password $SASL_PASSWORD --topic $TOPIC --msg_size 128000 --produce_msgs 0 --rand_read_msgs 1 --seq_read=0 --parallel 64

``` 