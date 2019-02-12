# Demo commands - from release folder - or installation

# Server:

Add `--poll-mode` at the end if you want lower latency (higher cpu usage)

```sh

./src/v/redpanda/redpanda --redpanda-cfg $(git rev-parse --show-toplevel)/src/v/redpanda/sample-cfg.yml --cpuset 0-3 -m 40G

```


# Client:

## All writes:

* 200-byte (key=value) pairs. 
* Each RPC is `write-batch-size`
* We dispatch `qps` per core per second (each request is a _batch_)
* With `concurrency` sockets per core
* For `seconds-duration` in seconds
* With threads pinned to `cpuset`
* `rw-balance 1.0` means all writes
* Using `-m20G` 20 gigs of ram.
* Add `--poll-mode` at the end for lower latency.

```sh

./src/v/redpanda/bamboo/bamboo --key-size 20 --value-size 180 --write-batch-size 1024 --qps 1024 --concurrency 4 --seconds-duration 1 --cpuset 4-7 --rw-balance 1.0 -m20G

```

Run the same workload w/ kafka

```
# 1. start zookeeper via
sudo systemctl  start zookeeper

# 2. run the client (bounded - same load as above of 1024)
time ./bin/kafka-run-class.sh org.apache.kafka.tools.ProducerPerformance --print-metrics --topic test --num-records 4194304  --record-size 200 --throughput -1 --producer-props acks=1 batch.size=1024 bootstrap.servers=localhost:9092


# 3. Results w/ kafka 2.1.0 on Feb 4, 2019
4194304 records sent, 435590.819400 records/sec (83.08 MB/sec), 255.51 ms avg latency, 757.00 ms max latency, 242 ms 50th, 481 ms 95th, 621 ms 99th, 744 ms 99.9th.

# 4. read 4 GB (times out at times)
time kafka_2.11-2.1.0/bin/kafka-run-class.sh kafka.tools.ConsumerPerformance --print-metrics --reporting-interval 1000 --topic test --group $(openssl rand -base64 15) --broker-list localhost:9092 --messages 4193040 --num-fetch-threads 4 --show-detailed-stats  --threads 4 --timeout 10000

```


In total this would yield:

### Throughput
`4 (cores) * 1024 (qps) * 1 (seconds-duration) * 1024 (batch)` messages/second
With this configuration we can do in 1.1seconds 4'194'304 messages. ~= 4MMqps

#### Throughput business notes vs Kafka

* Usual savings of 4-5X in throughput.
* In dollars per machine if you are throughput bound, per year it's 8K per computer.
* That's ~30K per year for 3 computers saved. 
* Most deployments are at 10 computers or lager.

[aws kakfa pricing](https://aws.amazon.com/msk/pricing/)

### Latency

Let's do a sustained load of 10 seconds. (smaller batch of 512 records) (512*1024*4) ~2MMqps (sustained for 10secs)

```sh
./src/v/redpanda/bamboo/bamboo --key-size 20 --value-size 180 --write-batch-size 512 --qps 1024 --concurrency 4 --seconds-duration 10 --cpuset 4-7 --rw-balance 1.0 -m20G

```

Throughput for 10 secs sustained ~2MMqps

|percentile | latency ms |
|-----------|------------|
|p10        |  51        | 
|p50        |  209       |
|p90        |  331       |
|p99        |  458       |
|p999       |  499       |
|p100       |  513       |


### Latency w/ smaller batches



```sh
./src/v/redpanda/bamboo/bamboo --key-size 20 --value-size 180 --write-batch-size 1 --qps 1024 --concurrency 4 --seconds-duration 10 --cpuset 4-7 --rw-balance 1.0 -m20G

```


Throughput for 10 secs sustained 4096qps

|percentile | latency ms |
|-----------|------------|
|p10        |  1.4       | 
|p50        |  2.6       |
|p90        |  3.6       |
|p99        |  7.8       |
|p999       |  11.5      |
|p100       |  11.5      |


### Latency grand finale

with *DPDK* you get 10x lower latency for all cases above.

#### Latency business notes vs Kafka

* If you are latency bound this is 10000% (ten thousand percent) faster
* 100x if you prefer
* In practice Akamai had 100ms tail latency SLA's.
* Kafka could only meet that w/ an overprovision of 6-8X computers. 
* Financially given 10 computers as a base deployment. 
* That's 60 computers (for a load that 10 would do) 
* Savings of ~600K. (at Amazong managed kafka prices)
* Obviously Akamai doesn't pay that. Real savings for Akamai would be ~300K


# Reads

Show the ~2GB reads per second

```sh
./src/v/redpanda/bamboo/bamboo --key-size 20 --value-size 180 --write-batch-size 1 --qps 512 --concurrency 4 --seconds-duration 1 --cpuset 4-7 --rw-balance 0.0 -m20G

```

* `qps` 512 (1 MB per request. 512MB of user data. 548MB of actual bytes transferred). Sends 512 requests per second
* over 4 (`concurrency`)
* on `cpuset` 4-7 (4 cores)
* `0.0` means 100% reads
* `-m20G` means 20G for the client side (not really used, just copied the command from above) 

## Throughput 

That's ~2GB/sec

## Latency

|percentile | latency ms |
|-----------|------------|
|p10        |  38        | 
|p50        |  124       |
|p90        |  171       |
|p99        |  250       |
|p999       |  276       |
|p100       |  279       |


### Throughput business notes vs Kafka

* Kakfa can only do per broker ~300MB - same hardware 
* 6x more read throughput
* Similar savings as above. 
* 6X of 10 computers is ~300K in dollars for reads for Akamai 


### Latency w/ smaller batches:



```sh
./src/v/redpanda/bamboo/bamboo --key-size 20 --value-size 180 --write-batch-size 1 --qps 1 --concurrency 4 --seconds-duration 10 --cpuset 4-7 --rw-balance 0.0 -m20G

```

|percentile | latency ms |
|-----------|------------|
|p10        |  0.2       | 
|p50        |  1.6       |
|p90        |  21.1      |
|p99        |  24.3      |
|p999       |  24.6      |
|p100       |  24.6      |


### Throughput business notes vs Kafka

* 16X (vs 400ms p100) *lower/better* latency
* _unlocks_ products that kafka cannot power


### Read latency grand finale

*DPDK* can deliver 10x lower latency on all of these numbers.

* Allows us to deliver effectively line (NVMe SSD) rate for writes
* and NIC (tested on DPDK ixgbe10 driver) speeds on reads (2ms tail -slowest- reads)


# kafka scripts:

Run the kafka server 

```
./kakfa/bin/start-server v/tools/kafka/server.properties
```

Run the client


