
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

### How Validation Works

kgo-verifier validates data integrity by embedding metadata in each message and tracking
which offsets contain valid data:

**Message Structure**: Each produced record includes a header `KGO_VERIFIER_RECORD_ID`
with format `producerId.offset` containing the offset where the producer expects the
message to land. The message key typically matches this value (unless using
`--key-set-cardinality` for compactible data).

**Valid Offset Tracking**: The producer tracks which messages successfully committed at
their expected offsets and writes this to `valid_offsets_{topic}.json`. This file contains
offset ranges per partition that are known to contain valid data. Consumers load this file
and use it to distinguish between validation failures (bugs) and expected anomalies
(retried messages landing at different offsets).

**Validation on Read**: Consumers validate each record by:
- Checking that the header value matches the expected format for that offset
- Verifying monotonicity: offsets and leader epochs must increase
- Detecting gaps in offset sequences (unless topic is compacted)
- Counting valid reads, invalid reads, and out-of-scope invalid reads (expected anomalies)

**Compacted Topic Support**: For compacted topics (`--compacted`), offset gaps are tolerated.
With `--validate-latest-values`, the producer also writes `latest_value_{topic}.json`
containing the most recent value for each key, and consumers verify that consumed values
match the latest produced values.

### Worker Types

kgo-verifier operates in different modes via worker types:

**Producer Worker** (`--produce_msgs N`): Produces N messages with validation metadata.
Supports transactional production (`--use-transactions`), tombstones
(`--tombstone-probability`), configurable key cardinality for compaction testing
(`--key-set-cardinality`), and producer ID churning (`--msgs-per-producer-id`).

**Sequential Read Worker** (`--seq_read`): Consumes the entire topic sequentially from
beginning to end, validating each message. Can be combined with `--loop` to continuously
re-read from the start, or `--continuous` to wait for new messages after reaching the end.

**Random Read Worker** (`--rand_read_msgs N`): Performs N random reads from random
offsets and partitions. Can run multiple workers in parallel with `--parallel`.
Particularly useful for stressing tiered storage caches.

**Consumer Group Worker** (`--consumer_group_readers N`): Runs N consumers in a
consumer group, distributing partition consumption across the members. Supports
offset committing with `--max-uncommitted` to control commit frequency.

### Key Features

- **Transaction Support**: Use `--use-transactions` to enable transactional production
  with configurable abort rate (`--transaction-abort-rate`) and batch size
  (`--msgs-per-transaction`). Messages in aborted transactions are marked and validated
  as unreadable.

- **Throughput Control**: Rate limit producer with `--produce-throughput-bps` or
  consumers with `--consume-throughput-mb`.

- **Chaos Tolerance**: With `--tolerate-data-loss` and `--tolerate-failed-produce`,
  the tool can continue operating and validating through cluster instability.

- **Remote Control**: Use `--remote` to enable HTTP control endpoints for automated
  testing: `/status` (get metrics), `/reset` (reset statistics), `/shutdown` (stop
  gracefully), `/last_pass` (finish current pass), `/print_stack` (debug output).

- **Compression Testing**: Use `--compression-type` to test specific codecs, or `mixed`
  to randomly vary compression per producer.

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

#### 6. Consumer group with multiple members
Run multiple consumers in a consumer group, committing offsets after every 1000 records

    kgo-verifier --brokers $BROKERS --username $SASL_USER --password $SASL_PASSWORD --topic $TOPIC --msg_size 128000 --produce_msgs 0 --consumer_group_readers 4 --consumer_group_name mygroup --max-uncommitted 1000

#### 7. Testing compacted topics
Produce data with limited key cardinality and tombstones, then validate latest values

    # Produce with 100 unique keys and 10% tombstones
    kgo-verifier --brokers $BROKERS --topic $TOPIC --produce_msgs 10000 --key-set-cardinality 100 --tombstone-probability 0.1 --compacted

    # After compaction, validate that consumed values match latest produced
    kgo-verifier --brokers $BROKERS --topic $TOPIC --seq_read --compacted --validate-latest-values

#### 8. Testing transactions
Produce with transactions, aborting 20% of them

    kgo-verifier --brokers $BROKERS --topic $TOPIC --produce_msgs 10000 --use-transactions --transaction-abort-rate 0.2 --msgs-per-transaction 10

``` 