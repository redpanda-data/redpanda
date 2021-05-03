
## [1. Getting Started](#gettingStarted)

### [1.1 Introduction](#introduction)

<div class="p-introduction">
We do:

Graceful shutdown
Leadership
Geo-replication (Rename mirror-maker2)
</div>

### [1.2 Use Cases](#uses) [Separate page]

Here is a description of a few of the popular use cases for Apache
Kafka®. For an overview of a number of these areas in action, see [this
blog
post](https://engineering.linkedin.com/distributed-systems/log-what-every-software-engineer-should-know-about-real-time-datas-unifying/).

#### [Messaging](#uses_messaging)

Kafka works well as a replacement for a more traditional message broker.
Message brokers are used for a variety of reasons (to decouple
processing from data producers, to buffer unprocessed messages, etc). In
comparison to most messaging systems Kafka has better throughput,
built-in partitioning, replication, and fault-tolerance which makes it a
good solution for large scale message processing applications.

In our experience messaging uses are often comparatively low-throughput,
but may require low end-to-end latency and often depend on the strong
durability guarantees Kafka provides.

In this domain Kafka is comparable to traditional messaging systems such
as [ActiveMQ](http://activemq.apache.org) or
[RabbitMQ](https://www.rabbitmq.com).

#### [Website Activity Tracking](#uses_website)

The original use case for Kafka was to be able to rebuild a user
activity tracking pipeline as a set of real-time publish-subscribe
feeds. This means site activity (page views, searches, or other actions
users may take) is published to central topics with one topic per
activity type. These feeds are available for subscription for a range of
use cases including real-time processing, real-time monitoring, and
loading into Hadoop or offline data warehousing systems for offline
processing and reporting.

Activity tracking is often very high volume as many activity messages
are generated for each user page view.

#### [Metrics](#uses_metrics)

Kafka is often used for operational monitoring data. This involves
aggregating statistics from distributed applications to produce
centralized feeds of operational data.

#### [Log Aggregation](#uses_logs)

Many people use Kafka as a replacement for a log aggregation solution.
Log aggregation typically collects physical log files off servers and
puts them in a central place (a file server or HDFS perhaps) for
processing. Kafka abstracts away the details of files and gives a
cleaner abstraction of log or event data as a stream of messages. This
allows for lower-latency processing and easier support for multiple data
sources and distributed data consumption. In comparison to log-centric
systems like Scribe or Flume, Kafka offers equally good performance,
stronger durability guarantees due to replication, and much lower
end-to-end latency.

#### [Stream Processing](#uses_streamprocessing)

Many users of Kafka process data in processing pipelines consisting of
multiple stages, where raw input data is consumed from Kafka topics and
then aggregated, enriched, or otherwise transformed into new topics for
further consumption or follow-up processing. For example, a processing
pipeline for recommending news articles might crawl article content from
RSS feeds and publish it to an "articles" topic; further processing
might normalize or deduplicate this content and publish the cleansed
article content to a new topic; a final processing stage might attempt
to recommend this content to users. Such processing pipelines create
graphs of real-time data flows based on the individual topics. Starting
in 0.10.0.0, a light-weight but powerful stream processing library
called [Kafka Streams](/documentation/streams) is available in Apache
Kafka to perform such data processing as described above. Apart from
Kafka Streams, alternative open source stream processing tools include
[Apache Storm](https://storm.apache.org/) and [Apache
Samza](http://samza.apache.org/).

#### [Event Sourcing](#uses_eventsourcing)

[Event sourcing](http://martinfowler.com/eaaDev/EventSourcing.html) is a
style of application design where state changes are logged as a
time-ordered sequence of records. Kafka's support for very large stored
log data makes it an excellent backend for an application built in this
style.

#### [Commit Log](#uses_commitlog)

Kafka can serve as a kind of external commit-log for a distributed
system. The log helps replicate data between nodes and acts as a
re-syncing mechanism for failed nodes to restore their data. The [log
compaction](/documentation.html#compaction) feature in Kafka helps
support this usage. In this usage Kafka is similar to [Apache
BookKeeper](https://bookkeeper.apache.org/) project.

### [1.4 Ecosystem](#ecosystem) [Separate page]

There are a plethora of tools that integrate with Kafka outside the main
distribution. The [ecosystem
page](https://cwiki.apache.org/confluence/display/KAFKA/Ecosystem) lists
many of these, including stream processing systems, Hadoop integration,
monitoring, and deployment tools.

### [1.5 Upgrading From Previous Versions](#upgrade) [Future]

## [2. APIs](#api)

<div class="p-api">

</div>

## [6. Operations](#operations)

Here is some information on actually running Kafka as a production
system based on usage and experience at LinkedIn. Please send us any
additional tips you know of.

### [6.1 Basic Kafka Operations](#basic_ops)

This section will review the most common operations you will perform on
your Kafka cluster. All of the tools reviewed in this section are
available under the `bin/` directory of the Kafka distribution and each
tool will print details on all possible commandline options if it is run
with no arguments.

#### [Adding and removing topics](#basic_ops_add_topic)

You have the option of either adding topics manually or having them be
created automatically when data is first published to a non-existent
topic. If topics are auto-created then you may want to tune the default
[topic configurations](#topicconfigs) used for auto-created topics.

Topics are added and modified using the topic tool:

``` line-numbers
  > bin/kafka-topics.sh --bootstrap-server broker_host:port --create --topic my_topic_name \
        --partitions 20 --replication-factor 3 --config x=y
```

The replication factor controls how many servers will replicate each
message that is written. If you have a replication factor of 3 then up
to 2 servers can fail before you will lose access to your data. We
recommend you use a replication factor of 2 or 3 so that you can
transparently bounce machines without interrupting data consumption.

The partition count controls how many logs the topic will be sharded
into. There are several impacts of the partition count. First each
partition must fit entirely on a single server. So if you have 20
partitions the full data set (and read and write load) will be handled
by no more than 20 servers (not counting replicas). Finally the
partition count impacts the maximum parallelism of your consumers. This
is discussed in greater detail in the [concepts
section](#intro_consumers).

Each sharded partition log is placed into its own folder under the Kafka
log directory. The name of such folders consists of the topic name,
appended by a dash (-) and the partition id. Since a typical folder name
can not be over 255 characters long, there will be a limitation on the
length of topic names. We assume the number of partitions will not ever
be above 100,000. Therefore, topic names cannot be longer than 249
characters. This leaves just enough room in the folder name for a dash
and a potentially 5 digit long partition id.

The configurations added on the command line override the default
settings the server has for things like the length of time data should
be retained. The complete set of per-topic configurations is documented
[here](#topicconfigs).

#### [Modifying topics](#basic_ops_modify_topic)

You can change the configuration or partitioning of a topic using the
same topic tool.

To add partitions you can do

``` line-numbers
  > bin/kafka-topics.sh --bootstrap-server broker_host:port --alter --topic my_topic_name \
        --partitions 40
```

Be aware that one use case for partitions is to semantically partition
data, and adding partitions doesn't change the partitioning of existing
data so this may disturb consumers if they rely on that partition. That
is if data is partitioned by `hash(key) % number_of_partitions` then
this partitioning will potentially be shuffled by adding partitions but
Kafka will not attempt to automatically redistribute data in any way.

To add configs:

``` line-numbers
  > bin/kafka-configs.sh --bootstrap-server broker_host:port --entity-type topics --entity-name my_topic_name --alter --add-config x=y
```

To remove a config:

``` line-numbers
  > bin/kafka-configs.sh --bootstrap-server broker_host:port --entity-type topics --entity-name my_topic_name --alter --delete-config x
```

And finally deleting a topic:

``` line-numbers
  > bin/kafka-topics.sh --bootstrap-server broker_host:port --delete --topic my_topic_name
```

Kafka does not currently support reducing the number of partitions for a
topic.

Instructions for changing the replication factor of a topic can be found
[here](#basic_ops_increase_replication_factor).

#### [Rack Awareness](#basic_ops_racks) [Simplify, Q2]

The rack awareness feature spreads replicas of the same partition across
different racks. This extends the guarantees Kafka provides for
broker-failure to cover rack-failure, limiting the risk of data loss
should all the brokers on a rack fail at once. The feature can also be
applied to other broker groupings such as availability zones in EC2.

You can specify that a broker belongs to a particular rack by adding a
property to the broker config:

``` language-text
  broker.rack=my-rack-id
```

When a topic is [created](#basic_ops_add_topic),
[modified](#basic_ops_modify_topic) or replicas are
[redistributed](#basic_ops_cluster_expansion), the rack constraint will
be honoured, ensuring replicas span as many racks as they can (a
partition will span min(\#racks, replication-factor) different racks).

The algorithm used to assign replicas to brokers ensures that the number
of leaders per broker will be constant, regardless of how brokers are
distributed across racks. This ensures balanced throughput.

However if racks are assigned different numbers of brokers, the
assignment of replicas will not be even. Racks with fewer brokers will
get more replicas, meaning they will use more storage and put more
resources into replication. Hence it is sensible to configure an equal
number of brokers per rack.

#### [Mirroring data between clusters & Geo-replication](#basic_ops_mirror_maker)

Kafka administrators can define data flows that cross the boundaries of
individual Kafka clusters, data centers, or geographical regions. Please
refer to the section on [Geo-Replication](#georeplication) for further
information.

#### [CHecking consumer lag](#basic_ops_consumer_lag)

`rpk cluster offsets`

#### [Expanding your cluster](#basic_ops_cluster_expansion) [Noah, Michal for May]

Adding servers to a Kafka cluster is easy, just assign them a unique
broker id and start up Kafka on your new servers. However these new
servers will not automatically be assigned any data partitions, so
unless partitions are moved to them they won't be doing any work until
new topics are created. So usually when you add machines to your cluster
you will want to migrate some existing data to these machines.

The process of migrating data is manually initiated but fully automated.
Under the covers what happens is that Kafka will add the new server as a
follower of the partition it is migrating and allow it to fully
replicate the existing data in that partition. When the new server has
fully replicated the contents of this partition and joined the in-sync
replica one of the existing replicas will delete their partition's data.

The partition reassignment tool can be used to move partitions across
brokers. An ideal partition distribution would ensure even data load and
partition sizes across all brokers. The partition reassignment tool does
not have the capability to automatically study the data distribution in
a Kafka cluster and move partitions around to attain an even load
distribution. As such, the admin has to figure out which topics or
partitions should be moved around.

The partition reassignment tool can run in 3 mutually exclusive modes:

  - \--generate: In this mode, given a list of topics and a list of
    brokers, the tool generates a candidate reassignment to move all
    partitions of the specified topics to the new brokers. This option
    merely provides a convenient way to generate a partition
    reassignment plan given a list of topics and target brokers.
  - \--execute: In this mode, the tool kicks off the reassignment of
    partitions based on the user provided reassignment plan. (using the
    --reassignment-json-file option). This can either be a custom
    reassignment plan hand crafted by the admin or provided by using the
    --generate option
  - \--verify: In this mode, the tool verifies the status of the
    reassignment for all partitions listed during the last --execute.
    The status can be either of successfully completed, failed or in
    progress

##### [Automatically migrating data to new machines](#basic_ops_automigrate) [Noah, Michal for May]

The partition reassignment tool can be used to move some topics off of
the current set of brokers to the newly added brokers. This is typically
useful while expanding an existing cluster since it is easier to move
entire topics to the new set of brokers, than moving one partition at a
time. When used to do this, the user should provide a list of topics
that should be moved to the new set of brokers and a target list of new
brokers. The tool then evenly distributes all partitions for the given
list of topics across the new set of brokers. During this move, the
replication factor of the topic is kept constant. Effectively the
replicas for all partitions for the input list of topics are moved from
the old set of brokers to the newly added brokers.

For instance, the following example will move all partitions for topics
foo1,foo2 to the new set of brokers 5,6. At the end of this move, all
partitions for topics foo1 and foo2 will *only* exist on brokers 5,6.

Since the tool accepts the input list of topics as a json file, you
first need to identify the topics you want to move and create the json
file as follows:

``` line-numbers
  > cat topics-to-move.json
  {"topics": [{"topic": "foo1"},
              {"topic": "foo2"}],
  "version":1
  }
```

Once the json file is ready, use the partition reassignment tool to
generate a candidate assignment:

``` line-numbers
  > bin/kafka-reassign-partitions.sh --bootstrap-server localhost:9092 --topics-to-move-json-file topics-to-move.json --broker-list "5,6" --generate
  Current partition replica assignment

  {"version":1,
  "partitions":[{"topic":"foo1","partition":2,"replicas":[1,2]},
                {"topic":"foo1","partition":0,"replicas":[3,4]},
                {"topic":"foo2","partition":2,"replicas":[1,2]},
                {"topic":"foo2","partition":0,"replicas":[3,4]},
                {"topic":"foo1","partition":1,"replicas":[2,3]},
                {"topic":"foo2","partition":1,"replicas":[2,3]}]
  }

  Proposed partition reassignment configuration

  {"version":1,
  "partitions":[{"topic":"foo1","partition":2,"replicas":[5,6]},
                {"topic":"foo1","partition":0,"replicas":[5,6]},
                {"topic":"foo2","partition":2,"replicas":[5,6]},
                {"topic":"foo2","partition":0,"replicas":[5,6]},
                {"topic":"foo1","partition":1,"replicas":[5,6]},
                {"topic":"foo2","partition":1,"replicas":[5,6]}]
  }
```

The tool generates a candidate assignment that will move all partitions
from topics foo1,foo2 to brokers 5,6. Note, however, that at this point,
the partition movement has not started, it merely tells you the current
assignment and the proposed new assignment. The current assignment
should be saved in case you want to rollback to it. The new assignment
should be saved in a json file (e.g. expand-cluster-reassignment.json)
to be input to the tool with the --execute option as follows:

``` line-numbers
  > bin/kafka-reassign-partitions.sh --bootstrap-server localhost:9092 --reassignment-json-file expand-cluster-reassignment.json --execute
  Current partition replica assignment

  {"version":1,
  "partitions":[{"topic":"foo1","partition":2,"replicas":[1,2]},
                {"topic":"foo1","partition":0,"replicas":[3,4]},
                {"topic":"foo2","partition":2,"replicas":[1,2]},
                {"topic":"foo2","partition":0,"replicas":[3,4]},
                {"topic":"foo1","partition":1,"replicas":[2,3]},
                {"topic":"foo2","partition":1,"replicas":[2,3]}]
  }

  Save this to use as the --reassignment-json-file option during rollback
  Successfully started reassignment of partitions
  {"version":1,
  "partitions":[{"topic":"foo1","partition":2,"replicas":[5,6]},
                {"topic":"foo1","partition":0,"replicas":[5,6]},
                {"topic":"foo2","partition":2,"replicas":[5,6]},
                {"topic":"foo2","partition":0,"replicas":[5,6]},
                {"topic":"foo1","partition":1,"replicas":[5,6]},
                {"topic":"foo2","partition":1,"replicas":[5,6]}]
  }
```

Finally, the --verify option can be used with the tool to check the
status of the partition reassignment. Note that the same
expand-cluster-reassignment.json (used with the --execute option) should
be used with the --verify option:

``` line-numbers
  > bin/kafka-reassign-partitions.sh --bootstrap-server localhost:9092 --reassignment-json-file expand-cluster-reassignment.json --verify
  Status of partition reassignment:
  Reassignment of partition [foo1,0] completed successfully
  Reassignment of partition [foo1,1] is in progress
  Reassignment of partition [foo1,2] is in progress
  Reassignment of partition [foo2,0] completed successfully
  Reassignment of partition [foo2,1] completed successfully
  Reassignment of partition [foo2,2] completed successfully
```

##### [Custom partition assignment and migration](#basic_ops_partitionassignment) [Noah, Michal for May]

The partition reassignment tool can also be used to selectively move
replicas of a partition to a specific set of brokers. When used in this
manner, it is assumed that the user knows the reassignment plan and does
not require the tool to generate a candidate reassignment, effectively
skipping the --generate step and moving straight to the --execute step

For instance, the following example moves partition 0 of topic foo1 to
brokers 5,6 and partition 1 of topic foo2 to brokers 2,3:

The first step is to hand craft the custom reassignment plan in a json
file:

``` line-numbers
  > cat custom-reassignment.json
  {"version":1,"partitions":[{"topic":"foo1","partition":0,"replicas":[5,6]},{"topic":"foo2","partition":1,"replicas":[2,3]}]}
```

Then, use the json file with the --execute option to start the
reassignment process:

``` line-numbers
  > bin/kafka-reassign-partitions.sh --bootstrap-server localhost:9092 --reassignment-json-file custom-reassignment.json --execute
  Current partition replica assignment

  {"version":1,
  "partitions":[{"topic":"foo1","partition":0,"replicas":[1,2]},
                {"topic":"foo2","partition":1,"replicas":[3,4]}]
  }

  Save this to use as the --reassignment-json-file option during rollback
  Successfully started reassignment of partitions
  {"version":1,
  "partitions":[{"topic":"foo1","partition":0,"replicas":[5,6]},
                {"topic":"foo2","partition":1,"replicas":[2,3]}]
  }
```

The --verify option can be used with the tool to check the status of the
partition reassignment. Note that the same custom-reassignment.json
(used with the --execute option) should be used with the --verify
option:

``` line-numbers
  > bin/kafka-reassign-partitions.sh --bootstrap-server localhost:9092 --reassignment-json-file custom-reassignment.json --verify
  Status of partition reassignment:
  Reassignment of partition [foo1,0] completed successfully
  Reassignment of partition [foo2,1] completed successfully
```

#### [Decommissioning brokers](#basic_ops_decommissioning_brokers) [Noah, Michal for May]

The partition reassignment tool does not have the ability to
automatically generate a reassignment plan for decommissioning brokers
yet. As such, the admin has to come up with a reassignment plan to move
the replica for all partitions hosted on the broker to be
decommissioned, to the rest of the brokers. This can be relatively
tedious as the reassignment needs to ensure that all the replicas are
not moved from the decommissioned broker to only one other broker. To
make this process effortless, we plan to add tooling support for
decommissioning brokers in the future.

#### [Increasing replication factor](#basic_ops_increase_replication_factor) [Noah, Michal for May]

Increasing the replication factor of an existing partition is easy. Just
specify the extra replicas in the custom reassignment json file and use
it with the --execute option to increase the replication factor of the
specified partitions.

For instance, the following example increases the replication factor of
partition 0 of topic foo from 1 to 3. Before increasing the replication
factor, the partition's only replica existed on broker 5. As part of
increasing the replication factor, we will add more replicas on brokers
6 and 7.

The first step is to hand craft the custom reassignment plan in a json
file:

``` line-numbers
  > cat increase-replication-factor.json
  {"version":1,
  "partitions":[{"topic":"foo","partition":0,"replicas":[5,6,7]}]}
```

Then, use the json file with the --execute option to start the
reassignment process:

``` line-numbers
  > bin/kafka-reassign-partitions.sh --bootstrap-server localhost:9092 --reassignment-json-file increase-replication-factor.json --execute
  Current partition replica assignment

  {"version":1,
  "partitions":[{"topic":"foo","partition":0,"replicas":[5]}]}

  Save this to use as the --reassignment-json-file option during rollback
  Successfully started reassignment of partitions
  {"version":1,
  "partitions":[{"topic":"foo","partition":0,"replicas":[5,6,7]}]}
```

The --verify option can be used with the tool to check the status of the
partition reassignment. Note that the same
increase-replication-factor.json (used with the --execute option) should
be used with the --verify option:

``` line-numbers
  > bin/kafka-reassign-partitions.sh --bootstrap-server localhost:9092 --reassignment-json-file increase-replication-factor.json --verify
  Status of partition reassignment:
  Reassignment of partition [foo,0] completed successfully
```

You can also verify the increase in replication factor with the
kafka-topics tool:

``` line-numbers
  > bin/kafka-topics.sh --bootstrap-server localhost:9092 --topic foo --describe
  Topic:foo PartitionCount:1    ReplicationFactor:3 Configs:
    Topic: foo  Partition: 0    Leader: 5   Replicas: 5,6,7 Isr: 5,6,7
```

#### [Setting quotas](#quotas) [Future]

Quotas overrides and defaults may be configured at (user, client-id),
user or client-id levels as described [here](#design_quotas). By
default, clients receive an unlimited quota. It is possible to set
custom quotas for each (user, client-id), user or client-id group.

Configure custom quota for (user=user1, client-id=clientA):

``` line-numbers
  > bin/kafka-configs.sh  --bootstrap-server localhost:9092 --alter --add-config 'producer_byte_rate=1024,consumer_byte_rate=2048,request_percentage=200' --entity-type users --entity-name user1 --entity-type clients --entity-name clientA
  Updated config for entity: user-principal 'user1', client-id 'clientA'.
```

Configure custom quota for user=user1:

``` line-numbers
  > bin/kafka-configs.sh  --bootstrap-server localhost:9092 --alter --add-config 'producer_byte_rate=1024,consumer_byte_rate=2048,request_percentage=200' --entity-type users --entity-name user1
  Updated config for entity: user-principal 'user1'.
```

Configure custom quota for client-id=clientA:

``` line-numbers
  > bin/kafka-configs.sh  --bootstrap-server localhost:9092 --alter --add-config 'producer_byte_rate=1024,consumer_byte_rate=2048,request_percentage=200' --entity-type clients --entity-name clientA
  Updated config for entity: client-id 'clientA'.
```

It is possible to set default quotas for each (user, client-id), user or
client-id group by specifying *--entity-default* option instead of
*--entity-name*.

Configure default client-id quota for user=userA:

``` line-numbers
  > bin/kafka-configs.sh  --bootstrap-server localhost:9092 --alter --add-config 'producer_byte_rate=1024,consumer_byte_rate=2048,request_percentage=200' --entity-type users --entity-name user1 --entity-type clients --entity-default
  Updated config for entity: user-principal 'user1', default client-id.
```

Configure default quota for user:

``` line-numbers
  > bin/kafka-configs.sh  --bootstrap-server localhost:9092 --alter --add-config 'producer_byte_rate=1024,consumer_byte_rate=2048,request_percentage=200' --entity-type users --entity-default
  Updated config for entity: default user-principal.
```

Configure default quota for client-id:

``` line-numbers
  > bin/kafka-configs.sh  --bootstrap-server localhost:9092 --alter --add-config 'producer_byte_rate=1024,consumer_byte_rate=2048,request_percentage=200' --entity-type clients --entity-default
  Updated config for entity: default client-id.
```

Here's how to describe the quota for a given (user, client-id):

``` line-numbers
  > bin/kafka-configs.sh  --bootstrap-server localhost:9092 --describe --entity-type users --entity-name user1 --entity-type clients --entity-name clientA
  Configs for user-principal 'user1', client-id 'clientA' are producer_byte_rate=1024,consumer_byte_rate=2048,request_percentage=200
```

Describe quota for a given user:

``` line-numbers
  > bin/kafka-configs.sh  --bootstrap-server localhost:9092 --describe --entity-type users --entity-name user1
  Configs for user-principal 'user1' are producer_byte_rate=1024,consumer_byte_rate=2048,request_percentage=200
```

Describe quota for a given client-id:

``` line-numbers
  > bin/kafka-configs.sh  --bootstrap-server localhost:9092 --describe --entity-type clients --entity-name clientA
  Configs for client-id 'clientA' are producer_byte_rate=1024,consumer_byte_rate=2048,request_percentage=200
```

If entity name is not specified, all entities of the specified type are
described. For example, describe all users:

``` line-numbers
  > bin/kafka-configs.sh  --bootstrap-server localhost:9092 --describe --entity-type users
  Configs for user-principal 'user1' are producer_byte_rate=1024,consumer_byte_rate=2048,request_percentage=200
  Configs for default user-principal are producer_byte_rate=1024,consumer_byte_rate=2048,request_percentage=200
```

Similarly for (user, client):

``` line-numbers
  > bin/kafka-configs.sh  --bootstrap-server localhost:9092 --describe --entity-type users --entity-type clients
  Configs for user-principal 'user1', default client-id are producer_byte_rate=1024,consumer_byte_rate=2048,request_percentage=200
  Configs for user-principal 'user1', client-id 'clientA' are producer_byte_rate=1024,consumer_byte_rate=2048,request_percentage=200
```

It is possible to set default quotas that apply to all client-ids by
setting these configs on the brokers. These properties are applied only
if quota overrides or defaults are not configured in Zookeeper. By
default, each client-id receives an unlimited quota. The following sets
the default quota per producer and consumer client-id to 10MB/sec.

``` line-numbers
    quota.producer.default=10485760
    quota.consumer.default=10485760
```

Note that these properties are being deprecated and may be removed in a
future release. Defaults configured using kafka-configs.sh take
precedence over these properties.

### [6.4 Multi-Tenancy](#multitenancy)

#### [Multi-Tenancy Overview](#multitenancy-overview)

As a highly scalable event streaming platform, Kafka is used by many
users as their central nervous system, connecting in real-time a wide
range of different systems and applications from various teams and lines
of businesses. Such multi-tenant cluster environments command proper
control and management to ensure the peaceful coexistence of these
different needs. This section highlights features and best practices to
set up such shared environments, which should help you operate clusters
that meet SLAs/OLAs and that minimize potential collateral damage caused
by "noisy neighbors".

Multi-tenancy is a many-sided subject, including but not limited to:

  - Creating user spaces for tenants (sometimes called namespaces)
  - Configuring topics with data retention policies and more
  - Securing topics and clusters with encryption, authentication, and
    authorization
  - Isolating tenants with quotas and rate limits
  - Monitoring and metering
  - Inter-cluster data sharing (cf. geo-replication)

#### [Creating User Spaces (Namespaces) For Tenants With Topic Naming](#multitenancy-topic-naming)

Kafka administrators operating a multi-tenant cluster typically need to
define user spaces for each tenant. For the purpose of this section,
"user spaces" are a collection of topics, which are grouped together
under the management of a single entity or user.

In Kafka, the main unit of data is the topic. Users can create and name
each topic. They can also delete them, but it is not possible to rename
a topic directly. Instead, to rename a topic, the user must create a new
topic, move the messages from the original topic to the new, and then
delete the original. With this in mind, it is recommended to define
logical spaces, based on an hierarchical topic naming structure. This
setup can then be combined with security features, such as prefixed
ACLs, to isolate different spaces and tenants, while also minimizing the
administrative overhead for securing the data in the cluster.

These logical user spaces can be grouped in different ways, and the
concrete choice depends on how your organization prefers to use your
Kafka clusters. The most common groupings are as follows.

*By team or organizational unit:* Here, the team is the main aggregator.
In an organization where teams are the main user of the Kafka
infrastructure, this might be the best grouping.

Example topic naming structure:

  - `<organization>.<team>.<dataset>.<event-name>`  
    (e.g., "acme.infosec.telemetry.logins")

*By project or product:* Here, a team manages more than one project.
Their credentials will be different for each project, so all the
controls and settings will always be project related.

Example topic naming structure:

  - `<project>.<product>.<event-name>`  
    (e.g., "mobility.payments.suspicious")

Certain information should normally not be put in a topic name, such as
information that is likely to change over time (e.g., the name of the
intended consumer) or that is a technical detail or metadata that is
available elsewhere (e.g., the topic's partition count and other
configuration settings).

To enforce a topic naming structure, several options are available:

  - Use [prefix ACLs](#security_authz)
    to enforce a common prefix for topic names. For example, team A may
    only be permitted to create topics whose names start with
    `payments.teamA.`.
  - Disable topic creation for normal users by denying it with an ACL,
    and then rely on an external process to create topics on behalf of
    users (e.g., scripting or your favorite automation toolkit).
  - It may also be useful to disable the Kafka feature to auto-create # Different name for config parameter
    topics on demand by setting `auto.create.topics.enable=false` in the
    broker configuration. Note that you should not rely solely on this
    option.

#### [Configuring Topics: Data Retention And More](#multitenancy-topic-configs)

Kafka's configuration is very flexible due to its fine granularity, and
it supports a plethora of [per-topic configuration
settings](#topicconfigs) to help administrators set up multi-tenant
clusters. For example, administrators often need to define data
retention policies to control how much and/or for how long data will be
stored in a topic, with settings such as
[retention.bytes](#retention.bytes) (size) and
[retention.ms](#retention.ms) (time). This limits storage consumption
within the cluster, and helps complying with legal requirements such as
GDPR.

#### [Securing Clusters and Topics: Authentication, Authorization, Encryption](#multitenancy-security)

Because the documentation has a dedicated chapter on
[security](#security) that applies to any Kafka deployment, this section
focuses on additional considerations for multi-tenant environments.

Security settings for Kafka fall into three main categories, which are
similar to how administrators would secure other client-server data
systems, like relational databases and traditional messaging systems.

1.  **Encryption** of data transferred between Kafka brokers and Kafka
    clients, between brokers, between brokers and ZooKeeper nodes, and
    between brokers and other, optional tools.
2.  **Authentication** of connections from Kafka clients and
    applications to Kafka brokers, as well as connections from Kafka
    brokers to ZooKeeper nodes.
3.  **Authorization** of client operations such as creating, deleting,
    and altering the configuration of topics; writing events to or
    reading events from a topic; creating and deleting ACLs.
    Administrators can also define custom policies to put in place
    additional restrictions, such as a `CreateTopicPolicy` and
    `AlterConfigPolicy` (see
    [KIP-108](https://cwiki.apache.org/confluence/display/KAFKA/KIP-108%3A+Create+Topic+Policy)
    and the settings
    [create.topic.policy.class.name](#brokerconfigs_create.topic.policy.class.name),
    [alter.config.policy.class.name](#brokerconfigs_alter.config.policy.class.name)).

When securing a multi-tenant Kafka environment, the most common
administrative task is the third category (authorization), i.e.,
managing the user/client permissions that grant or deny access to
certain topics and thus to the data stored by users within a cluster.
This task is performed predominantly through the [setting of access
control lists (ACLs)](#security_authz). Here, administrators of
multi-tenant environments in particular benefit from putting a
hierarchical topic naming structure in place as described in a previous
section, because they can conveniently control access to topics through
prefixed ACLs (`--resource-pattern-type Prefixed`). This significantly
minimizes the administrative overhead of securing topics in multi-tenant
environments: administrators can make their own trade-offs between
higher developer convenience (more lenient permissions, using fewer and
broader ACLs) vs. tighter security (more stringent permissions, using
more and narrower ACLs).

In the following example, user Alice—a new member of ACME corporation's
InfoSec team—is granted write permissions to all topics whose names
start with "acme.infosec.", such as "acme.infosec.telemetry.logins" and
"acme.infosec.syslogs.events".

``` line-numbers
# Grant permissions to user Alice
$ bin/kafka-acls.sh \
    --bootstrap-server broker1:9092 \
    --add --allow-principal User:Alice \
    --producer \
    --resource-pattern-type prefixed --topic acme.infosec.
```

You can similarly use this approach to isolate different customers on
the same shared cluster.

#### [Isolating Tenants: Quotas, Rate Limiting, Throttling](#multitenancy-isolation)

Multi-tenant clusters should generally be configured with
[quotas](#design_quotas), which protect against users (tenants) eating
up too many cluster resources, such as when they attempt to write or
read very high volumes of data, or create requests to brokers at an
excessively high rate. This may cause network saturation, monopolize
broker resources, and impact other clients—all of which you want to
avoid in a shared environment.

**Client quotas:** Kafka supports different types of (per-user
principal) client quotas. Because a client's quotas apply irrespective
of which topics the client is writing to or reading from, they are a
convenient and effective tool to allocate resources in a multi-tenant
cluster. [Request rate quotas](#design_quotascpu), for example, help to
limit a user's impact on broker CPU usage by limiting the time a broker
spends on the [request handling path](/protocol.html) for that user,
after which throttling kicks in. In many situations, isolating users
with request rate quotas has a bigger impact in multi-tenant clusters
than setting incoming/outgoing network bandwidth quotas, because
excessive broker CPU usage for processing requests reduces the effective
bandwidth the broker can serve. Furthermore, administrators can also
define quotas on topic operations—such as create, delete, and alter—to
prevent Kafka clusters from being overwhelmed by highly concurrent topic
operations (see
[KIP-599](https://cwiki.apache.org/confluence/display/KAFKA/KIP-599%3A+Throttle+Create+Topic%2C+Create+Partition+and+Delete+Topic+Operations)
and the quota type `controller_mutations_rate`).

**Server quotas:** Kafka also supports different types of broker-side
quotas. For example, administrators can set a limit on the rate with
which the [broker accepts new
connections](#brokerconfigs_max.connection.creation.rate), set the
[maximum number of connections per
broker](#brokerconfigs_max.connections), or set the maximum number of
connections allowed [from a specific IP
address](#brokerconfigs_max.connections.per.ip).

For more information, please refer to the [quota
overview](#design_quotas) and [how to set quotas](#quotas).

#### [Monitoring and Metering](#multitenancy-monitoring)

[Monitoring](#monitoring) is a broader subject that is covered
[elsewhere](#monitoring) in the documentation. Administrators of any
Kafka environment, but especially multi-tenant ones, should set up
monitoring according to these instructions. Kafka supports a wide range
of metrics, such as the rate of failed authentication attempts, request
latency, consumer lag, total number of consumer groups, metrics on the
quotas described in the previous section, and many more.

For example, monitoring can be configured to track the size of
topic-partitions (with the JMX metric
`kafka.log.Log.Size.<TOPIC-NAME>`), and thus the total size of data
stored in a topic. You can then define alerts when tenants on shared
clusters are getting close to using too much storage space.

#### [Multi-Tenancy and Geo-Replication](#multitenancy-georeplication)

Kafka lets you share data across different clusters, which may be
located in different geographical regions, data centers, and so on.
Apart from use cases such as disaster recovery, this functionality is
useful when a multi-tenant setup requires inter-cluster data sharing.
See the section [Geo-Replication (Cross-Cluster Data
Mirroring)](#georeplication) for more information.

#### [Further considerations](#multitenancy-more)

**Data contracts:** You may need to define data contracts between the
producers and the consumers of data in a cluster, using event schemas.
This ensures that events written to Kafka can always be read properly
again, and prevents malformed or corrupt events being written. The best
way to achieve this is to deploy a so-called schema registry alongside
the cluster. (Kafka does not include a schema registry, but there are
third-party implementations available.) A schema registry manages the
event schemas and maps the schemas to topics, so that producers know
which topics are accepting which types (schemas) of events, and
consumers know how to read and parse events in a topic. Some registry
implementations provide further functionality, such as schema evolution,
storing a history of all schemas, and schema compatibility settings.

### [6.5 Kafka Configuration](#config)

#### [Important Client Configurations](#clientconfig)

The most important producer configurations are:

  - acks
  - compression
  - batch size

The most important consumer configuration is the fetch size.

All configurations are documented in the [configuration](#configuration)
section.

#### [A Production Server Config](#prodconfig)

Here is an example production server configuration:

``` line-numbers
  # ZooKeeper
  zookeeper.connect=[list of ZooKeeper servers]

  # Log configuration
  num.partitions=8
  default.replication.factor=3
  log.dir=[List of directories. Kafka should have its own dedicated disk(s) or SSD(s).]

  # Other configurations
  broker.id=[An integer. Start with 0 and increment by 1 for each new broker.]
  listeners=[list of listeners]
  auto.create.topics.enable=false
  min.insync.replicas=2
  queued.max.requests=[number of concurrent requests]
```

Our client configuration varies a fair amount between different use
cases.

### [6.6 Java Version](#java)

Java 8 and Java 11 are supported. Java 11 performs significantly better
if TLS is enabled, so it is highly recommended (it also includes a
number of other performance improvements: G1GC, CRC32C, Compact Strings,
Thread-Local Handshakes and more). From a security perspective, we
recommend the latest released patch version as older freely available
versions have disclosed security vulnerabilities. Typical arguments for
running Kafka with OpenJDK-based Java implementations (including Oracle
JDK) are:

``` line-numbers
  -Xmx6g -Xms6g -XX:MetaspaceSize=96m -XX:+UseG1GC
  -XX:MaxGCPauseMillis=20 -XX:InitiatingHeapOccupancyPercent=35 -XX:G1HeapRegionSize=16M
  -XX:MinMetaspaceFreeRatio=50 -XX:MaxMetaspaceFreeRatio=80 -XX:+ExplicitGCInvokesConcurrent
```

For reference, here are the stats for one of LinkedIn's busiest clusters
(at peak) that uses said Java arguments:

  - 60 brokers
  - 50k partitions (replication factor 2)
  - 800k messages/sec in
  - 300 MB/sec inbound, 1 GB/sec+ outbound

All of the brokers in that cluster have a 90% GC pause time of about
21ms with less than 1 young GC per second.

### [6.7 Hardware and OS](#hwandos)

We are using dual quad-core Intel Xeon machines with 24GB of memory.

You need sufficient memory to buffer active readers and writers. You can
do a back-of-the-envelope estimate of memory needs by assuming you want
to be able to buffer for 30 seconds and compute your memory need as
write\_throughput\*30.

The disk throughput is important. We have 8x7200 rpm SATA drives. In
general disk throughput is the performance bottleneck, and more disks is
better. Depending on how you configure flush behavior you may or may not
benefit from more expensive disks (if you force flush often then higher
RPM SAS drives may be better).

#### [OS](#os)

Kafka should run well on any unix system and has been tested on Linux
and Solaris.

We have seen a few issues running on Windows and Windows is not
currently a well supported platform though we would be happy to change
that.

It is unlikely to require much OS-level tuning, but there are three
potentially important OS-level configurations:

  - File descriptor limits: Kafka uses file descriptors for log segments
    and open connections. If a broker hosts many partitions, consider
    that the broker needs at least
    (number\_of\_partitions)\*(partition\_size/segment\_size) to track
    all log segments in addition to the number of connections the broker
    makes. We recommend at least 100000 allowed file descriptors for the
    broker processes as a starting point. Note: The mmap() function adds
    an extra reference to the file associated with the file descriptor
    fildes which is not removed by a subsequent close() on that file
    descriptor. This reference is removed when there are no more
    mappings to the file.
  - Max socket buffer size: can be increased to enable high-performance
    data transfer between data centers as [described
    here](http://www.psc.edu/index.php/networking/641-tcp-tune).
  - Maximum number of memory map areas a process may have (aka
    vm.max\_map\_count). [See the Linux kernel
    documentation](http://kernel.org/doc/Documentation/sysctl/vm.txt).
    You should keep an eye at this OS-level property when considering
    the maximum number of partitions a broker may have. By default, on a
    number of Linux systems, the value of vm.max\_map\_count is
    somewhere around 65535. Each log segment, allocated per partition,
    requires a pair of index/timeindex files, and each of these files
    consumes 1 map area. In other words, each log segment uses 2 map
    areas. Thus, each partition requires minimum 2 map areas, as long as
    it hosts a single log segment. That is to say, creating 50000
    partitions on a broker will result allocation of 100000 map areas
    and likely cause broker crash with OutOfMemoryError (Map failed) on
    a system with default vm.max\_map\_count. Keep in mind that the
    number of log segments per partition varies depending on the segment
    size, load intensity, retention policy and, generally, tends to be
    more than one.

#### [Disks and Filesystem](#diskandfs)

We recommend using multiple drives to get good throughput and not
sharing the same drives used for Kafka data with application logs or
other OS filesystem activity to ensure good latency. You can either RAID
these drives together into a single volume or format and mount each
drive as its own directory. Since Kafka has replication the redundancy
provided by RAID can also be provided at the application level. This
choice has several tradeoffs.

If you configure multiple data directories partitions will be assigned
round-robin to data directories. Each partition will be entirely in one
of the data directories. If data is not well balanced among partitions
this can lead to load imbalance between disks.

RAID can potentially do better at balancing load between disks (although
it doesn't always seem to) because it balances load at a lower level.
The primary downside of RAID is that it is usually a big performance hit
for write throughput and reduces the available disk space.

Another potential benefit of RAID is the ability to tolerate disk
failures. However our experience has been that rebuilding the RAID array
is so I/O intensive that it effectively disables the server, so this
does not provide much real availability improvement.

#### [Application vs. OS Flush Management](#appvsosflush)

Kafka always immediately writes all data to the filesystem and supports
the ability to configure the flush policy that controls when data is
forced out of the OS cache and onto disk using the flush. This flush
policy can be controlled to force data to disk after a period of time or
after a certain number of messages has been written. There are several
choices in this configuration.

Kafka must eventually call fsync to know that data was flushed. When
recovering from a crash for any log segment not known to be fsync'd
Kafka will check the integrity of each message by checking its CRC and
also rebuild the accompanying offset index file as part of the recovery
process executed on startup.

Note that durability in Kafka does not require syncing data to disk, as
a failed node will always recover from its replicas.

We recommend using the default flush settings which disable application
fsync entirely. This means relying on the background flush done by the
OS and Kafka's own background flush. This provides the best of all
worlds for most uses: no knobs to tune, great throughput and latency,
and full recovery guarantees. We generally feel that the guarantees
provided by replication are stronger than sync to local disk, however
the paranoid still may prefer having both and application level fsync
policies are still supported.

The drawback of using application level flush settings is that it is
less efficient in its disk usage pattern (it gives the OS less leeway to
re-order writes) and it can introduce latency as fsync in most Linux
filesystems blocks writes to the file whereas the background flushing
does much more granular page-level locking.

In general you don't need to do any low-level tuning of the filesystem,
but in the next few sections we will go over some of this in case it is
useful.

#### [Understanding Linux OS Flush Behavior](#linuxflush)

In Linux, data written to the filesystem is maintained in
[pagecache](http://en.wikipedia.org/wiki/Page_cache) until it must be
written out to disk (due to an application-level fsync or the OS's own
flush policy). The flushing of data is done by a set of background
threads called pdflush (or in post 2.6.32 kernels "flusher threads").

Pdflush has a configurable policy that controls how much dirty data can
be maintained in cache and for how long before it must be written back
to disk. This policy is described
[here](http://web.archive.org/web/20160518040713/http://www.westnet.com/~gsmith/content/linux-pdflush.htm).
When Pdflush cannot keep up with the rate of data being written it will
eventually cause the writing process to block incurring latency in the
writes to slow down the accumulation of data.

You can see the current state of OS memory usage by doing

``` language-bash
 > cat /proc/meminfo 
```

The meaning of these values are described in the link above.

Using pagecache has several advantages over an in-process cache for
storing data that will be written out to disk:

  - The I/O scheduler will batch together consecutive small writes into
    bigger physical writes which improves throughput.
  - The I/O scheduler will attempt to re-sequence writes to minimize
    movement of the disk head which improves throughput.
  - It automatically uses all the free memory on the machine

#### [Filesystem Selection](#filesystems)

Kafka uses regular files on disk, and as such it has no hard dependency
on a specific filesystem. The two filesystems which have the most usage,
however, are EXT4 and XFS. Historically, EXT4 has had more usage, but
recent improvements to the XFS filesystem have shown it to have better
performance characteristics for Kafka's workload with no compromise in
stability.

Comparison testing was performed on a cluster with significant message
loads, using a variety of filesystem creation and mount options. The
primary metric in Kafka that was monitored was the "Request Local Time",
indicating the amount of time append operations were taking. XFS
resulted in much better local times (160ms vs. 250ms+ for the best EXT4
configuration), as well as lower average wait times. The XFS performance
also showed less variability in disk performance.

##### [General Filesystem Notes](#generalfs)

For any filesystem used for data directories, on Linux systems, the
following options are recommended to be used at mount time:

  - noatime: This option disables updating of a file's atime (last
    access time) attribute when the file is read. This can eliminate a
    significant number of filesystem writes, especially in the case of
    bootstrapping consumers. Kafka does not rely on the atime attributes
    at all, so it is safe to disable this.

##### [XFS Notes](#xfs)

The XFS filesystem has a significant amount of auto-tuning in place, so
it does not require any change in the default settings, either at
filesystem creation time or at mount. The only tuning parameters worth
considering are:

  - largeio: This affects the preferred I/O size reported by the stat
    call. While this can allow for higher performance on larger disk
    writes, in practice it had minimal or no effect on performance.
  - nobarrier: For underlying devices that have battery-backed cache,
    this option can provide a little more performance by disabling
    periodic write flushes. However, if the underlying device is
    well-behaved, it will report to the filesystem that it does not
    require flushes, and this option will have no effect.

##### [EXT4 Notes](#ext4)

EXT4 is a serviceable choice of filesystem for the Kafka data
directories, however getting the most performance out of it will require
adjusting several mount options. In addition, these options are
generally unsafe in a failure scenario, and will result in much more
data loss and corruption. For a single broker failure, this is not much
of a concern as the disk can be wiped and the replicas rebuilt from the
cluster. In a multiple-failure scenario, such as a power outage, this
can mean underlying filesystem (and therefore data) corruption that is
not easily recoverable. The following options can be adjusted:

  - data=writeback: Ext4 defaults to data=ordered which puts a strong
    order on some writes. Kafka does not require this ordering as it
    does very paranoid data recovery on all unflushed log. This setting
    removes the ordering constraint and seems to significantly reduce
    latency.
  - Disabling journaling: Journaling is a tradeoff: it makes reboots
    faster after server crashes but it introduces a great deal of
    additional locking which adds variance to write performance. Those
    who don't care about reboot time and want to reduce a major source
    of write latency spikes can turn off journaling entirely.
  - commit=num\_secs: This tunes the frequency with which ext4 commits
    to its metadata journal. Setting this to a lower value reduces the
    loss of unflushed data during a crash. Setting this to a higher
    value will improve throughput.
  - nobh: This setting controls additional ordering guarantees when
    using data=writeback mode. This should be safe with Kafka as we do
    not depend on write ordering and improves throughput and latency.
  - delalloc: Delayed allocation means that the filesystem avoid
    allocating any blocks until the physical write occurs. This allows
    ext4 to allocate a large extent instead of smaller pages and helps
    ensure the data is written sequentially. This feature is great for
    throughput. It does seem to involve some locking in the filesystem
    which adds a bit of latency variance.

### [6.8 Monitoring](#monitoring)

Kafka uses Yammer Metrics for metrics reporting in the server. The Java
clients use Kafka Metrics, a built-in metrics registry that minimizes
transitive dependencies pulled into client applications. Both expose
metrics via JMX and can be configured to report stats using pluggable
stats reporters to hook up to your monitoring system.

All Kafka rate metrics have a corresponding cumulative count metric with
suffix `-total`. For example, `records-consumed-rate` has a
corresponding metric named `records-consumed-total`.

The easiest way to see the available metrics is to fire up jconsole and
point it at a running kafka client or server; this will allow browsing
all metrics with JMX.

#### [Security Considerations for Remote Monitoring using JMX](#remote_jmx)

Apache Kafka disables remote JMX by default. You can enable remote
monitoring using JMX by setting the environment variable `JMX_PORT` for
processes started using the CLI or standard Java system properties to
enable remote JMX programmatically. You must enable security when
enabling remote JMX in production scenarios to ensure that unauthorized
users cannot monitor or control your broker or application as well as
the platform on which these are running. Note that authentication is
disabled for JMX by default in Kafka and security configs must be
overridden for production deployments by setting the environment
variable `KAFKA_JMX_OPTS` for processes started using the CLI or by
setting appropriate Java system properties. See [Monitoring and
Management Using JMX
Technology](https://docs.oracle.com/javase/8/docs/technotes/guides/management/agent.html)
for details on securing JMX.

We do graphing and alerting on the following metrics:

| Description                                                                                                                                                                                                   | Mbean name                                                                                                                                   | Normal value                                                                                                                                                                                                                                                                                                                                                                                                                                |
| ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Message in rate                                                                                                                                                                                               | kafka.server:type=BrokerTopicMetrics,name=MessagesInPerSec                                                                                   |                                                                                                                                                                                                                                                                                                                                                                                                                                             |
| Byte in rate from clients                                                                                                                                                                                     | kafka.server:type=BrokerTopicMetrics,name=BytesInPerSec                                                                                      |                                                                                                                                                                                                                                                                                                                                                                                                                                             |
| Byte in rate from other brokers                                                                                                                                                                               | kafka.server:type=BrokerTopicMetrics,name=ReplicationBytesInPerSec                                                                           |                                                                                                                                                                                                                                                                                                                                                                                                                                             |
| Request rate                                                                                                                                                                                                  | kafka.network:type=RequestMetrics,name=RequestsPerSec,request={Produce|FetchConsumer|FetchFollower},version=(\[0-9\]+)                       |                                                                                                                                                                                                                                                                                                                                                                                                                                             |
| Error rate                                                                                                                                                                                                    | kafka.network:type=RequestMetrics,name=ErrorsPerSec,request=(\[-.\\w\]+),error=(\[-.\\w\]+)                                                  | Number of errors in responses counted per-request-type, per-error-code. If a response contains multiple errors, all are counted. error=NONE indicates successful responses.                                                                                                                                                                                                                                                                 |
| Request size in bytes                                                                                                                                                                                         | kafka.network:type=RequestMetrics,name=RequestBytes,request=(\[-.\\w\]+)                                                                     | Size of requests for each request type.                                                                                                                                                                                                                                                                                                                                                                                                     |
| Temporary memory size in bytes                                                                                                                                                                                | kafka.network:type=RequestMetrics,name=TemporaryMemoryBytes,request={Produce|Fetch}                                                          | Temporary memory used for message format conversions and decompression.                                                                                                                                                                                                                                                                                                                                                                     |
| Message conversion time                                                                                                                                                                                       | kafka.network:type=RequestMetrics,name=MessageConversionsTimeMs,request={Produce|Fetch}                                                      | Time in milliseconds spent on message format conversions.                                                                                                                                                                                                                                                                                                                                                                                   |
| Message conversion rate                                                                                                                                                                                       | kafka.server:type=BrokerTopicMetrics,name={Produce|Fetch}MessageConversionsPerSec,topic=(\[-.\\w\]+)                                         | Number of records which required message format conversion.                                                                                                                                                                                                                                                                                                                                                                                 |
| Request Queue Size                                                                                                                                                                                            | kafka.network:type=RequestChannel,name=RequestQueueSize                                                                                      | Size of the request queue.                                                                                                                                                                                                                                                                                                                                                                                                                  |
| Byte out rate to clients                                                                                                                                                                                      | kafka.server:type=BrokerTopicMetrics,name=BytesOutPerSec                                                                                     |                                                                                                                                                                                                                                                                                                                                                                                                                                             |
| Byte out rate to other brokers                                                                                                                                                                                | kafka.server:type=BrokerTopicMetrics,name=ReplicationBytesOutPerSec                                                                          |                                                                                                                                                                                                                                                                                                                                                                                                                                             |
| Message validation failure rate due to no key specified for compacted topic                                                                                                                                   | kafka.server:type=BrokerTopicMetrics,name=NoKeyCompactedTopicRecordsPerSec                                                                   |                                                                                                                                                                                                                                                                                                                                                                                                                                             |
| Message validation failure rate due to invalid magic number                                                                                                                                                   | kafka.server:type=BrokerTopicMetrics,name=InvalidMagicNumberRecordsPerSec                                                                    |                                                                                                                                                                                                                                                                                                                                                                                                                                             |
| Message validation failure rate due to incorrect crc checksum                                                                                                                                                 | kafka.server:type=BrokerTopicMetrics,name=InvalidMessageCrcRecordsPerSec                                                                     |                                                                                                                                                                                                                                                                                                                                                                                                                                             |
| Message validation failure rate due to non-continuous offset or sequence number in batch                                                                                                                      | kafka.server:type=BrokerTopicMetrics,name=InvalidOffsetOrSequenceRecordsPerSec                                                               |                                                                                                                                                                                                                                                                                                                                                                                                                                             |
| Log flush rate and time                                                                                                                                                                                       | kafka.log:type=LogFlushStats,name=LogFlushRateAndTimeMs                                                                                      |                                                                                                                                                                                                                                                                                                                                                                                                                                             |
| \# of under replicated partitions (the number of non-reassigning replicas - the number of ISR \> 0)                                                                                                           | kafka.server:type=ReplicaManager,name=UnderReplicatedPartitions                                                                              | 0                                                                                                                                                                                                                                                                                                                                                                                                                                           |
| \# of under minIsr partitions (|ISR| \< min.insync.replicas)                                                                                                                                                  | kafka.server:type=ReplicaManager,name=UnderMinIsrPartitionCount                                                                              | 0                                                                                                                                                                                                                                                                                                                                                                                                                                           |
| \# of at minIsr partitions (|ISR| = min.insync.replicas)                                                                                                                                                      | kafka.server:type=ReplicaManager,name=AtMinIsrPartitionCount                                                                                 | 0                                                                                                                                                                                                                                                                                                                                                                                                                                           |
| \# of offline log directories                                                                                                                                                                                 | kafka.log:type=LogManager,name=OfflineLogDirectoryCount                                                                                      | 0                                                                                                                                                                                                                                                                                                                                                                                                                                           |
| Is controller active on broker                                                                                                                                                                                | kafka.controller:type=KafkaController,name=ActiveControllerCount                                                                             | only one broker in the cluster should have 1                                                                                                                                                                                                                                                                                                                                                                                                |
| Leader election rate                                                                                                                                                                                          | kafka.controller:type=ControllerStats,name=LeaderElectionRateAndTimeMs                                                                       | non-zero when there are broker failures                                                                                                                                                                                                                                                                                                                                                                                                     |
| Unclean leader election rate                                                                                                                                                                                  | kafka.controller:type=ControllerStats,name=UncleanLeaderElectionsPerSec                                                                      | 0                                                                                                                                                                                                                                                                                                                                                                                                                                           |
| Pending topic deletes                                                                                                                                                                                         | kafka.controller:type=KafkaController,name=TopicsToDeleteCount                                                                               |                                                                                                                                                                                                                                                                                                                                                                                                                                             |
| Pending replica deletes                                                                                                                                                                                       | kafka.controller:type=KafkaController,name=ReplicasToDeleteCount                                                                             |                                                                                                                                                                                                                                                                                                                                                                                                                                             |
| Ineligible pending topic deletes                                                                                                                                                                              | kafka.controller:type=KafkaController,name=TopicsIneligibleToDeleteCount                                                                     |                                                                                                                                                                                                                                                                                                                                                                                                                                             |
| Ineligible pending replica deletes                                                                                                                                                                            | kafka.controller:type=KafkaController,name=ReplicasIneligibleToDeleteCount                                                                   |                                                                                                                                                                                                                                                                                                                                                                                                                                             |
| Partition counts                                                                                                                                                                                              | kafka.server:type=ReplicaManager,name=PartitionCount                                                                                         | mostly even across brokers                                                                                                                                                                                                                                                                                                                                                                                                                  |
| Leader replica counts                                                                                                                                                                                         | kafka.server:type=ReplicaManager,name=LeaderCount                                                                                            | mostly even across brokers                                                                                                                                                                                                                                                                                                                                                                                                                  |
| ISR shrink rate                                                                                                                                                                                               | kafka.server:type=ReplicaManager,name=IsrShrinksPerSec                                                                                       | If a broker goes down, ISR for some of the partitions will shrink. When that broker is up again, ISR will be expanded once the replicas are fully caught up. Other than that, the expected value for both ISR shrink rate and expansion rate is 0.                                                                                                                                                                                          |
| ISR expansion rate                                                                                                                                                                                            | kafka.server:type=ReplicaManager,name=IsrExpandsPerSec                                                                                       | See above                                                                                                                                                                                                                                                                                                                                                                                                                                   |
| Max lag in messages btw follower and leader replicas                                                                                                                                                          | kafka.server:type=ReplicaFetcherManager,name=MaxLag,clientId=Replica                                                                         | lag should be proportional to the maximum batch size of a produce request.                                                                                                                                                                                                                                                                                                                                                                  |
| Lag in messages per follower replica                                                                                                                                                                          | kafka.server:type=FetcherLagMetrics,name=ConsumerLag,clientId=(\[-.\\w\]+),topic=(\[-.\\w\]+),partition=(\[0-9\]+)                           | lag should be proportional to the maximum batch size of a produce request.                                                                                                                                                                                                                                                                                                                                                                  |
| Requests waiting in the producer purgatory                                                                                                                                                                    | kafka.server:type=DelayedOperationPurgatory,name=PurgatorySize,delayedOperation=Produce                                                      | non-zero if ack=-1 is used                                                                                                                                                                                                                                                                                                                                                                                                                  |
| Requests waiting in the fetch purgatory                                                                                                                                                                       | kafka.server:type=DelayedOperationPurgatory,name=PurgatorySize,delayedOperation=Fetch                                                        | size depends on fetch.wait.max.ms in the consumer                                                                                                                                                                                                                                                                                                                                                                                           |
| Request total time                                                                                                                                                                                            | kafka.network:type=RequestMetrics,name=TotalTimeMs,request={Produce|FetchConsumer|FetchFollower}                                             | broken into queue, local, remote and response send time                                                                                                                                                                                                                                                                                                                                                                                     |
| Time the request waits in the request queue                                                                                                                                                                   | kafka.network:type=RequestMetrics,name=RequestQueueTimeMs,request={Produce|FetchConsumer|FetchFollower}                                      |                                                                                                                                                                                                                                                                                                                                                                                                                                             |
| Time the request is processed at the leader                                                                                                                                                                   | kafka.network:type=RequestMetrics,name=LocalTimeMs,request={Produce|FetchConsumer|FetchFollower}                                             |                                                                                                                                                                                                                                                                                                                                                                                                                                             |
| Time the request waits for the follower                                                                                                                                                                       | kafka.network:type=RequestMetrics,name=RemoteTimeMs,request={Produce|FetchConsumer|FetchFollower}                                            | non-zero for produce requests when ack=-1                                                                                                                                                                                                                                                                                                                                                                                                   |
| Time the request waits in the response queue                                                                                                                                                                  | kafka.network:type=RequestMetrics,name=ResponseQueueTimeMs,request={Produce|FetchConsumer|FetchFollower}                                     |                                                                                                                                                                                                                                                                                                                                                                                                                                             |
| Time to send the response                                                                                                                                                                                     | kafka.network:type=RequestMetrics,name=ResponseSendTimeMs,request={Produce|FetchConsumer|FetchFollower}                                      |                                                                                                                                                                                                                                                                                                                                                                                                                                             |
| Number of messages the consumer lags behind the producer by. Published by the consumer, not broker.                                                                                                           | kafka.consumer:type=consumer-fetch-manager-metrics,client-id={client-id} Attribute: records-lag-max                                          |                                                                                                                                                                                                                                                                                                                                                                                                                                             |
| The average fraction of time the network processors are idle                                                                                                                                                  | kafka.network:type=SocketServer,name=NetworkProcessorAvgIdlePercent                                                                          | between 0 and 1, ideally \> 0.3                                                                                                                                                                                                                                                                                                                                                                                                             |
| The number of connections disconnected on a processor due to a client not re-authenticating and then using the connection beyond its expiration time for anything other than re-authentication                | kafka.server:type=socket-server-metrics,listener=\[SASL\_PLAINTEXT|SASL\_SSL\],networkProcessor=\<\#\>,name=expired-connections-killed-count | ideally 0 when re-authentication is enabled, implying there are no longer any older, pre-2.2.0 clients connecting to this (listener, processor) combination                                                                                                                                                                                                                                                                                 |
| The total number of connections disconnected, across all processors, due to a client not re-authenticating and then using the connection beyond its expiration time for anything other than re-authentication | kafka.network:type=SocketServer,name=ExpiredConnectionsKilledCount                                                                           | ideally 0 when re-authentication is enabled, implying there are no longer any older, pre-2.2.0 clients connecting to this broker                                                                                                                                                                                                                                                                                                            |
| The average fraction of time the request handler threads are idle                                                                                                                                             | kafka.server:type=KafkaRequestHandlerPool,name=RequestHandlerAvgIdlePercent                                                                  | between 0 and 1, ideally \> 0.3                                                                                                                                                                                                                                                                                                                                                                                                             |
| Bandwidth quota metrics per (user, client-id), user or client-id                                                                                                                                              | kafka.server:type={Produce|Fetch},user=(\[-.\\w\]+),client-id=(\[-.\\w\]+)                                                                   | Two attributes. throttle-time indicates the amount of time in ms the client was throttled. Ideally = 0. byte-rate indicates the data produce/consume rate of the client in bytes/sec. For (user, client-id) quotas, both user and client-id are specified. If per-client-id quota is applied to the client, user is not specified. If per-user quota is applied, client-id is not specified.                                                |
| Request quota metrics per (user, client-id), user or client-id                                                                                                                                                | kafka.server:type=Request,user=(\[-.\\w\]+),client-id=(\[-.\\w\]+)                                                                           | Two attributes. throttle-time indicates the amount of time in ms the client was throttled. Ideally = 0. request-time indicates the percentage of time spent in broker network and I/O threads to process requests from client group. For (user, client-id) quotas, both user and client-id are specified. If per-client-id quota is applied to the client, user is not specified. If per-user quota is applied, client-id is not specified. |
| Requests exempt from throttling                                                                                                                                                                               | kafka.server:type=Request                                                                                                                    | exempt-throttle-time indicates the percentage of time spent in broker network and I/O threads to process requests that are exempt from throttling.                                                                                                                                                                                                                                                                                          |
| ZooKeeper client request latency                                                                                                                                                                              | kafka.server:type=ZooKeeperClientMetrics,name=ZooKeeperRequestLatencyMs                                                                      | Latency in millseconds for ZooKeeper requests from broker.                                                                                                                                                                                                                                                                                                                                                                                  |
| ZooKeeper connection status                                                                                                                                                                                   | kafka.server:type=SessionExpireListener,name=SessionState                                                                                    | Connection status of broker's ZooKeeper session which may be one of Disconnected|SyncConnected|AuthFailed|ConnectedReadOnly|SaslAuthenticated|Expired.                                                                                                                                                                                                                                                                                      |
| Max time to load group metadata                                                                                                                                                                               | kafka.server:type=group-coordinator-metrics,name=partition-load-time-max                                                                     | maximum time, in milliseconds, it took to load offsets and group metadata from the consumer offset partitions loaded in the last 30 seconds (including time spent waiting for the loading task to be scheduled)                                                                                                                                                                                                                             |
| Avg time to load group metadata                                                                                                                                                                               | kafka.server:type=group-coordinator-metrics,name=partition-load-time-avg                                                                     | average time, in milliseconds, it took to load offsets and group metadata from the consumer offset partitions loaded in the last 30 seconds (including time spent waiting for the loading task to be scheduled)                                                                                                                                                                                                                             |
| Max time to load transaction metadata                                                                                                                                                                         | kafka.server:type=transaction-coordinator-metrics,name=partition-load-time-max                                                               | maximum time, in milliseconds, it took to load transaction metadata from the consumer offset partitions loaded in the last 30 seconds (including time spent waiting for the loading task to be scheduled)                                                                                                                                                                                                                                   |
| Avg time to load transaction metadata                                                                                                                                                                         | kafka.server:type=transaction-coordinator-metrics,name=partition-load-time-avg                                                               | average time, in milliseconds, it took to load transaction metadata from the consumer offset partitions loaded in the last 30 seconds (including time spent waiting for the loading task to be scheduled)                                                                                                                                                                                                                                   |
| Consumer Group Offset Count                                                                                                                                                                                   | kafka.server:type=GroupMetadataManager,name=NumOffsets                                                                                       | Total number of committed offsets for Consumer Groups                                                                                                                                                                                                                                                                                                                                                                                       |
| Consumer Group Count                                                                                                                                                                                          | kafka.server:type=GroupMetadataManager,name=NumGroups                                                                                        | Total number of Consumer Groups                                                                                                                                                                                                                                                                                                                                                                                                             |
| Consumer Group Count, per State                                                                                                                                                                               | kafka.server:type=GroupMetadataManager,name=NumGroups\[PreparingRebalance,CompletingRebalance,Empty,Stable,Dead\]                            | The number of Consumer Groups in each state: PreparingRebalance, CompletingRebalance, Empty, Stable, Dead                                                                                                                                                                                                                                                                                                                                   |
| Number of reassigning partitions                                                                                                                                                                              | kafka.server:type=ReplicaManager,name=ReassigningPartitions                                                                                  | The number of reassigning leader partitions on a broker.                                                                                                                                                                                                                                                                                                                                                                                    |
| Outgoing byte rate of reassignment traffic                                                                                                                                                                    | kafka.server:type=BrokerTopicMetrics,name=ReassignmentBytesOutPerSec                                                                         |                                                                                                                                                                                                                                                                                                                                                                                                                                             |
| Incoming byte rate of reassignment traffic                                                                                                                                                                    | kafka.server:type=BrokerTopicMetrics,name=ReassignmentBytesInPerSec                                                                          |                                                                                                                                                                                                                                                                                                                                                                                                                                             |
| Size of a partition on disk (in bytes)                                                                                                                                                                        | kafka.log:type=Log,name=Size,topic=(\[-.\\w\]+),partition=(\[0-9\]+)                                                                         | The size of a partition on disk, measured in bytes.                                                                                                                                                                                                                                                                                                                                                                                         |
| Number of log segments in a partition                                                                                                                                                                         | kafka.log:type=Log,name=NumLogSegments,topic=(\[-.\\w\]+),partition=(\[0-9\]+)                                                               | The number of log segments in a partition.                                                                                                                                                                                                                                                                                                                                                                                                  |
| First offset in a partition                                                                                                                                                                                   | kafka.log:type=Log,name=LogStartOffset,topic=(\[-.\\w\]+),partition=(\[0-9\]+)                                                               | The first offset in a partition.                                                                                                                                                                                                                                                                                                                                                                                                            |
| Last offset in a partition                                                                                                                                                                                    | kafka.log:type=Log,name=LogEndOffset,topic=(\[-.\\w\]+),partition=(\[0-9\]+)                                                                 | The last offset in a partition.                                                                                                                                                                                                                                                                                                                                                                                                             |

#### [Common monitoring metrics for producer/consumer/connect/streams](#selector_monitoring)

The following metrics are available on
producer/consumer/connector/streams instances. For specific metrics,
please see following sections.

| Metric/Attribute name                     | Description                                                                                                                                         | Mbean name                                                                                            |
| ----------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------- |
| connection-close-rate                     | Connections closed per second in the window.                                                                                                        | kafka.\[producer|consumer|connect\]:type=\[producer|consumer|connect\]-metrics,client-id=(\[-.\\w\]+) |
| connection-close-total                    | Total connections closed in the window.                                                                                                             | kafka.\[producer|consumer|connect\]:type=\[producer|consumer|connect\]-metrics,client-id=(\[-.\\w\]+) |
| connection-creation-rate                  | New connections established per second in the window.                                                                                               | kafka.\[producer|consumer|connect\]:type=\[producer|consumer|connect\]-metrics,client-id=(\[-.\\w\]+) |
| connection-creation-total                 | Total new connections established in the window.                                                                                                    | kafka.\[producer|consumer|connect\]:type=\[producer|consumer|connect\]-metrics,client-id=(\[-.\\w\]+) |
| network-io-rate                           | The average number of network operations (reads or writes) on all connections per second.                                                           | kafka.\[producer|consumer|connect\]:type=\[producer|consumer|connect\]-metrics,client-id=(\[-.\\w\]+) |
| network-io-total                          | The total number of network operations (reads or writes) on all connections.                                                                        | kafka.\[producer|consumer|connect\]:type=\[producer|consumer|connect\]-metrics,client-id=(\[-.\\w\]+) |
| outgoing-byte-rate                        | The average number of outgoing bytes sent per second to all servers.                                                                                | kafka.\[producer|consumer|connect\]:type=\[producer|consumer|connect\]-metrics,client-id=(\[-.\\w\]+) |
| outgoing-byte-total                       | The total number of outgoing bytes sent to all servers.                                                                                             | kafka.\[producer|consumer|connect\]:type=\[producer|consumer|connect\]-metrics,client-id=(\[-.\\w\]+) |
| request-rate                              | The average number of requests sent per second.                                                                                                     | kafka.\[producer|consumer|connect\]:type=\[producer|consumer|connect\]-metrics,client-id=(\[-.\\w\]+) |
| request-total                             | The total number of requests sent.                                                                                                                  | kafka.\[producer|consumer|connect\]:type=\[producer|consumer|connect\]-metrics,client-id=(\[-.\\w\]+) |
| request-size-avg                          | The average size of all requests in the window.                                                                                                     | kafka.\[producer|consumer|connect\]:type=\[producer|consumer|connect\]-metrics,client-id=(\[-.\\w\]+) |
| request-size-max                          | The maximum size of any request sent in the window.                                                                                                 | kafka.\[producer|consumer|connect\]:type=\[producer|consumer|connect\]-metrics,client-id=(\[-.\\w\]+) |
| incoming-byte-rate                        | Bytes/second read off all sockets.                                                                                                                  | kafka.\[producer|consumer|connect\]:type=\[producer|consumer|connect\]-metrics,client-id=(\[-.\\w\]+) |
| incoming-byte-total                       | Total bytes read off all sockets.                                                                                                                   | kafka.\[producer|consumer|connect\]:type=\[producer|consumer|connect\]-metrics,client-id=(\[-.\\w\]+) |
| response-rate                             | Responses received per second.                                                                                                                      | kafka.\[producer|consumer|connect\]:type=\[producer|consumer|connect\]-metrics,client-id=(\[-.\\w\]+) |
| response-total                            | Total responses received.                                                                                                                           | kafka.\[producer|consumer|connect\]:type=\[producer|consumer|connect\]-metrics,client-id=(\[-.\\w\]+) |
| select-rate                               | Number of times the I/O layer checked for new I/O to perform per second.                                                                            | kafka.\[producer|consumer|connect\]:type=\[producer|consumer|connect\]-metrics,client-id=(\[-.\\w\]+) |
| select-total                              | Total number of times the I/O layer checked for new I/O to perform.                                                                                 | kafka.\[producer|consumer|connect\]:type=\[producer|consumer|connect\]-metrics,client-id=(\[-.\\w\]+) |
| io-wait-time-ns-avg                       | The average length of time the I/O thread spent waiting for a socket ready for reads or writes in nanoseconds.                                      | kafka.\[producer|consumer|connect\]:type=\[producer|consumer|connect\]-metrics,client-id=(\[-.\\w\]+) |
| io-wait-ratio                             | The fraction of time the I/O thread spent waiting.                                                                                                  | kafka.\[producer|consumer|connect\]:type=\[producer|consumer|connect\]-metrics,client-id=(\[-.\\w\]+) |
| io-time-ns-avg                            | The average length of time for I/O per select call in nanoseconds.                                                                                  | kafka.\[producer|consumer|connect\]:type=\[producer|consumer|connect\]-metrics,client-id=(\[-.\\w\]+) |
| io-ratio                                  | The fraction of time the I/O thread spent doing I/O.                                                                                                | kafka.\[producer|consumer|connect\]:type=\[producer|consumer|connect\]-metrics,client-id=(\[-.\\w\]+) |
| connection-count                          | The current number of active connections.                                                                                                           | kafka.\[producer|consumer|connect\]:type=\[producer|consumer|connect\]-metrics,client-id=(\[-.\\w\]+) |
| successful-authentication-rate            | Connections per second that were successfully authenticated using SASL or SSL.                                                                      | kafka.\[producer|consumer|connect\]:type=\[producer|consumer|connect\]-metrics,client-id=(\[-.\\w\]+) |
| successful-authentication-total           | Total connections that were successfully authenticated using SASL or SSL.                                                                           | kafka.\[producer|consumer|connect\]:type=\[producer|consumer|connect\]-metrics,client-id=(\[-.\\w\]+) |
| failed-authentication-rate                | Connections per second that failed authentication.                                                                                                  | kafka.\[producer|consumer|connect\]:type=\[producer|consumer|connect\]-metrics,client-id=(\[-.\\w\]+) |
| failed-authentication-total               | Total connections that failed authentication.                                                                                                       | kafka.\[producer|consumer|connect\]:type=\[producer|consumer|connect\]-metrics,client-id=(\[-.\\w\]+) |
| successful-reauthentication-rate          | Connections per second that were successfully re-authenticated using SASL.                                                                          | kafka.\[producer|consumer|connect\]:type=\[producer|consumer|connect\]-metrics,client-id=(\[-.\\w\]+) |
| successful-reauthentication-total         | Total connections that were successfully re-authenticated using SASL.                                                                               | kafka.\[producer|consumer|connect\]:type=\[producer|consumer|connect\]-metrics,client-id=(\[-.\\w\]+) |
| reauthentication-latency-max              | The maximum latency in ms observed due to re-authentication.                                                                                        | kafka.\[producer|consumer|connect\]:type=\[producer|consumer|connect\]-metrics,client-id=(\[-.\\w\]+) |
| reauthentication-latency-avg              | The average latency in ms observed due to re-authentication.                                                                                        | kafka.\[producer|consumer|connect\]:type=\[producer|consumer|connect\]-metrics,client-id=(\[-.\\w\]+) |
| failed-reauthentication-rate              | Connections per second that failed re-authentication.                                                                                               | kafka.\[producer|consumer|connect\]:type=\[producer|consumer|connect\]-metrics,client-id=(\[-.\\w\]+) |
| failed-reauthentication-total             | Total connections that failed re-authentication.                                                                                                    | kafka.\[producer|consumer|connect\]:type=\[producer|consumer|connect\]-metrics,client-id=(\[-.\\w\]+) |
| successful-authentication-no-reauth-total | Total connections that were successfully authenticated by older, pre-2.2.0 SASL clients that do not support re-authentication. May only be non-zero | kafka.\[producer|consumer|connect\]:type=\[producer|consumer|connect\]-metrics,client-id=(\[-.\\w\]+) |

#### [Common Per-broker metrics for producer/consumer/connect/streams](#common_node_monitoring)

The following metrics are available on
producer/consumer/connector/streams instances. For specific metrics,
please see following sections.

| Metric/Attribute name | Description                                                      | Mbean name                                                                                                                    |
| --------------------- | ---------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------- |
| outgoing-byte-rate    | The average number of outgoing bytes sent per second for a node. | kafka.\[producer|consumer|connect\]:type=\[consumer|producer|connect\]-node-metrics,client-id=(\[-.\\w\]+),node-id=(\[0-9\]+) |
| outgoing-byte-total   | The total number of outgoing bytes sent for a node.              | kafka.\[producer|consumer|connect\]:type=\[consumer|producer|connect\]-node-metrics,client-id=(\[-.\\w\]+),node-id=(\[0-9\]+) |
| request-rate          | The average number of requests sent per second for a node.       | kafka.\[producer|consumer|connect\]:type=\[consumer|producer|connect\]-node-metrics,client-id=(\[-.\\w\]+),node-id=(\[0-9\]+) |
| request-total         | The total number of requests sent for a node.                    | kafka.\[producer|consumer|connect\]:type=\[consumer|producer|connect\]-node-metrics,client-id=(\[-.\\w\]+),node-id=(\[0-9\]+) |
| request-size-avg      | The average size of all requests in the window for a node.       | kafka.\[producer|consumer|connect\]:type=\[consumer|producer|connect\]-node-metrics,client-id=(\[-.\\w\]+),node-id=(\[0-9\]+) |
| request-size-max      | The maximum size of any request sent in the window for a node.   | kafka.\[producer|consumer|connect\]:type=\[consumer|producer|connect\]-node-metrics,client-id=(\[-.\\w\]+),node-id=(\[0-9\]+) |
| incoming-byte-rate    | The average number of bytes received per second for a node.      | kafka.\[producer|consumer|connect\]:type=\[consumer|producer|connect\]-node-metrics,client-id=(\[-.\\w\]+),node-id=(\[0-9\]+) |
| incoming-byte-total   | The total number of bytes received for a node.                   | kafka.\[producer|consumer|connect\]:type=\[consumer|producer|connect\]-node-metrics,client-id=(\[-.\\w\]+),node-id=(\[0-9\]+) |
| request-latency-avg   | The average request latency in ms for a node.                    | kafka.\[producer|consumer|connect\]:type=\[consumer|producer|connect\]-node-metrics,client-id=(\[-.\\w\]+),node-id=(\[0-9\]+) |
| request-latency-max   | The maximum request latency in ms for a node.                    | kafka.\[producer|consumer|connect\]:type=\[consumer|producer|connect\]-node-metrics,client-id=(\[-.\\w\]+),node-id=(\[0-9\]+) |
| response-rate         | Responses received per second for a node.                        | kafka.\[producer|consumer|connect\]:type=\[consumer|producer|connect\]-node-metrics,client-id=(\[-.\\w\]+),node-id=(\[0-9\]+) |
| response-total        | Total responses received for a node.                             | kafka.\[producer|consumer|connect\]:type=\[consumer|producer|connect\]-node-metrics,client-id=(\[-.\\w\]+),node-id=(\[0-9\]+) |

#### [Producer monitoring](#producer_monitoring)

The following metrics are available on producer instances.

| Metric/Attribute name  | Description                                                                                        | Mbean name                                                  |
| ---------------------- | -------------------------------------------------------------------------------------------------- | ----------------------------------------------------------- |
| waiting-threads        | The number of user threads blocked waiting for buffer memory to enqueue their records.             | kafka.producer:type=producer-metrics,client-id=(\[-.\\w\]+) |
| buffer-total-bytes     | The maximum amount of buffer memory the client can use (whether or not it is currently used).      | kafka.producer:type=producer-metrics,client-id=(\[-.\\w\]+) |
| buffer-available-bytes | The total amount of buffer memory that is not being used (either unallocated or in the free list). | kafka.producer:type=producer-metrics,client-id=(\[-.\\w\]+) |
| bufferpool-wait-time   | The fraction of time an appender waits for space allocation.                                       | kafka.producer:type=producer-metrics,client-id=(\[-.\\w\]+) |

##### [Producer Sender Metrics](#producer_sender_monitoring)

| kafka.producer:type=producer-metrics,client-id="{client-id}" |  |  |
| --- | --- | --- |
|  | ATTRIBUTE NAME | DESCRIPTION |
|  | batch-size-avg | The average number of bytes sent per partition per-request. |
|  | batch-size-max | The max number of bytes sent per partition per-request. |
|  | batch-split-rate | The average number of batch splits per second |
|  | batch-split-total | The total number of batch splits |
|  | compression-rate-avg | The average compression rate of record batches, defined as the average ratio of the compressed batch size over the uncompressed size. |
|  | metadata-age | The age in seconds of the current producer metadata being used. |
|  | produce-throttle-time-avg | The average time in ms a request was throttled by a broker |
|  | produce-throttle-time-max | The maximum time in ms a request was throttled by a broker |
|  | record-error-rate | The average per-second number of record sends that resulted in errors |
|  | record-error-total | The total number of record sends that resulted in errors |
|  | record-queue-time-avg | The average time in ms record batches spent in the send buffer. |
|  | record-queue-time-max | The maximum time in ms record batches spent in the send buffer. |
|  | record-retry-rate | The average per-second number of retried record sends |
|  | record-retry-total | The total number of retried record sends |
|  | record-send-rate | The average number of records sent per second. |
|  | record-send-total | The total number of records sent. |
|  | record-size-avg | The average record size |
|  | record-size-max | The maximum record size |
|  | records-per-request-avg | The average number of records per request. |
|  | request-latency-avg | The average request latency in ms |
|  | request-latency-max | The maximum request latency in ms |
|  | requests-in-flight | The current number of in-flight requests awaiting a response. |
| kafka.producer:type=producer-topic-metrics,client-id="{client-id}",topic="{topic}" |  |  |
|  | ATTRIBUTE NAME | DESCRIPTION |
|  | byte-rate | The average number of bytes sent per second for a topic. |
|  | byte-total | The total number of bytes sent for a topic. |
|  | compression-rate | The average compression rate of record batches for a topic, defined as the average ratio of the compressed batch size over the uncompressed size. |
|  | record-error-rate | The average per-second number of record sends that resulted in errors for a topic |
|  | record-error-total | The total number of record sends that resulted in errors for a topic |
|  | record-retry-rate | The average per-second number of retried record sends for a topic |
|  | record-retry-total | The total number of retried record sends for a topic |
|  | record-send-rate | The average number of records sent per second for a topic. |
|  | record-send-total | The total number of records sent for a topic. |

#### [consumer monitoring](#consumer_monitoring)

The following metrics are available on consumer instances.

| Metric/Attribute name | Description                                                                                                            | Mbean name                                                  |
| --------------------- | ---------------------------------------------------------------------------------------------------------------------- | ----------------------------------------------------------- |
| time-between-poll-avg | The average delay between invocations of poll().                                                                       | kafka.consumer:type=consumer-metrics,client-id=(\[-.\\w\]+) |
| time-between-poll-max | The max delay between invocations of poll().                                                                           | kafka.consumer:type=consumer-metrics,client-id=(\[-.\\w\]+) |
| last-poll-seconds-ago | The number of seconds since the last poll() invocation.                                                                | kafka.consumer:type=consumer-metrics,client-id=(\[-.\\w\]+) |
| poll-idle-ratio-avg   | The average fraction of time the consumer's poll() is idle as opposed to waiting for the user code to process records. | kafka.consumer:type=consumer-metrics,client-id=(\[-.\\w\]+) |

##### [Consumer Group Metrics](#consumer_group_monitoring)

| Metric/Attribute name           | Description                                                                      | Mbean name                                                              |
| ------------------------------- | -------------------------------------------------------------------------------- | ----------------------------------------------------------------------- |
| commit-latency-avg              | The average time taken for a commit request                                      | kafka.consumer:type=consumer-coordinator-metrics,client-id=(\[-.\\w\]+) |
| commit-latency-max              | The max time taken for a commit request                                          | kafka.consumer:type=consumer-coordinator-metrics,client-id=(\[-.\\w\]+) |
| commit-rate                     | The number of commit calls per second                                            | kafka.consumer:type=consumer-coordinator-metrics,client-id=(\[-.\\w\]+) |
| commit-total                    | The total number of commit calls                                                 | kafka.consumer:type=consumer-coordinator-metrics,client-id=(\[-.\\w\]+) |
| assigned-partitions             | The number of partitions currently assigned to this consumer                     | kafka.consumer:type=consumer-coordinator-metrics,client-id=(\[-.\\w\]+) |
| heartbeat-response-time-max     | The max time taken to receive a response to a heartbeat request                  | kafka.consumer:type=consumer-coordinator-metrics,client-id=(\[-.\\w\]+) |
| heartbeat-rate                  | The average number of heartbeats per second                                      | kafka.consumer:type=consumer-coordinator-metrics,client-id=(\[-.\\w\]+) |
| heartbeat-total                 | The total number of heartbeats                                                   | kafka.consumer:type=consumer-coordinator-metrics,client-id=(\[-.\\w\]+) |
| join-time-avg                   | The average time taken for a group rejoin                                        | kafka.consumer:type=consumer-coordinator-metrics,client-id=(\[-.\\w\]+) |
| join-time-max                   | The max time taken for a group rejoin                                            | kafka.consumer:type=consumer-coordinator-metrics,client-id=(\[-.\\w\]+) |
| join-rate                       | The number of group joins per second                                             | kafka.consumer:type=consumer-coordinator-metrics,client-id=(\[-.\\w\]+) |
| join-total                      | The total number of group joins                                                  | kafka.consumer:type=consumer-coordinator-metrics,client-id=(\[-.\\w\]+) |
| sync-time-avg                   | The average time taken for a group sync                                          | kafka.consumer:type=consumer-coordinator-metrics,client-id=(\[-.\\w\]+) |
| sync-time-max                   | The max time taken for a group sync                                              | kafka.consumer:type=consumer-coordinator-metrics,client-id=(\[-.\\w\]+) |
| sync-rate                       | The number of group syncs per second                                             | kafka.consumer:type=consumer-coordinator-metrics,client-id=(\[-.\\w\]+) |
| sync-total                      | The total number of group syncs                                                  | kafka.consumer:type=consumer-coordinator-metrics,client-id=(\[-.\\w\]+) |
| rebalance-latency-avg           | The average time taken for a group rebalance                                     | kafka.consumer:type=consumer-coordinator-metrics,client-id=(\[-.\\w\]+) |
| rebalance-latency-max           | The max time taken for a group rebalance                                         | kafka.consumer:type=consumer-coordinator-metrics,client-id=(\[-.\\w\]+) |
| rebalance-latency-total         | The total time taken for group rebalances so far                                 | kafka.consumer:type=consumer-coordinator-metrics,client-id=(\[-.\\w\]+) |
| rebalance-total                 | The total number of group rebalances participated                                | kafka.consumer:type=consumer-coordinator-metrics,client-id=(\[-.\\w\]+) |
| rebalance-rate-per-hour         | The number of group rebalance participated per hour                              | kafka.consumer:type=consumer-coordinator-metrics,client-id=(\[-.\\w\]+) |
| failed-rebalance-total          | The total number of failed group rebalances                                      | kafka.consumer:type=consumer-coordinator-metrics,client-id=(\[-.\\w\]+) |
| failed-rebalance-rate-per-hour  | The number of failed group rebalance event per hour                              | kafka.consumer:type=consumer-coordinator-metrics,client-id=(\[-.\\w\]+) |
| last-rebalance-seconds-ago      | The number of seconds since the last rebalance event                             | kafka.consumer:type=consumer-coordinator-metrics,client-id=(\[-.\\w\]+) |
| last-heartbeat-seconds-ago      | The number of seconds since the last controller heartbeat                        | kafka.consumer:type=consumer-coordinator-metrics,client-id=(\[-.\\w\]+) |
| partitions-revoked-latency-avg  | The average time taken by the on-partitions-revoked rebalance listener callback  | kafka.consumer:type=consumer-coordinator-metrics,client-id=(\[-.\\w\]+) |
| partitions-revoked-latency-max  | The max time taken by the on-partitions-revoked rebalance listener callback      | kafka.consumer:type=consumer-coordinator-metrics,client-id=(\[-.\\w\]+) |
| partitions-assigned-latency-avg | The average time taken by the on-partitions-assigned rebalance listener callback | kafka.consumer:type=consumer-coordinator-metrics,client-id=(\[-.\\w\]+) |
| partitions-assigned-latency-max | The max time taken by the on-partitions-assigned rebalance listener callback     | kafka.consumer:type=consumer-coordinator-metrics,client-id=(\[-.\\w\]+) |
| partitions-lost-latency-avg     | The average time taken by the on-partitions-lost rebalance listener callback     | kafka.consumer:type=consumer-coordinator-metrics,client-id=(\[-.\\w\]+) |
| partitions-lost-latency-max     | The max time taken by the on-partitions-lost rebalance listener callback         | kafka.consumer:type=consumer-coordinator-metrics,client-id=(\[-.\\w\]+) |

##### [Consumer Fetch Metrics](#consumer_fetch_monitoring)

| kafka.consumer:type=consumer-fetch-manager-metrics,client-id="{client-id}" |  |  |
| --- | --- | --- |
|  | ATTRIBUTE NAME | DESCRIPTION |
|  | bytes-consumed-rate | The average number of bytes consumed per second |
|  | bytes-consumed-total | The total number of bytes consumed |
|  | fetch-latency-avg | The average time taken for a fetch request. |
|  | fetch-latency-max | The max time taken for any fetch request. |
|  | fetch-rate | The number of fetch requests per second. |
|  | fetch-size-avg | The average number of bytes fetched per request |
|  | fetch-size-max | The maximum number of bytes fetched per request |
|  | fetch-throttle-time-avg | The average throttle time in ms |
|  | fetch-throttle-time-max | The maximum throttle time in ms |
|  | fetch-total | The total number of fetch requests. |
|  | records-consumed-rate | The average number of records consumed per second |
|  | records-consumed-total | The total number of records consumed |
|  | records-lag-max | The maximum lag in terms of number of records for any partition in this window |
|  | records-lead-min | The minimum lead in terms of number of records for any partition in this window |
|  | records-per-request-avg | The average number of records in each request |
| kafka.consumer:type=consumer-fetch-manager-metrics,client-id="{client-id}",topic="{topic}" |  |  |
|  | ATTRIBUTE NAME | DESCRIPTION |
|  | bytes-consumed-rate | The average number of bytes consumed per second for a topic |
|  | bytes-consumed-total | The total number of bytes consumed for a topic |
|  | fetch-size-avg | The average number of bytes fetched per request for a topic |
|  | fetch-size-max | The maximum number of bytes fetched per request for a topic |
|  | records-consumed-rate | The average number of records consumed per second for a topic |
|  | records-consumed-total | The total number of records consumed for a topic |
|  | records-per-request-avg | The average number of records in each request for a topic |
| kafka.consumer:type=consumer-fetch-manager-metrics,partition="{partition}",topic="{topic}",client-id="{client-id}" |  |  |
|  | ATTRIBUTE NAME | DESCRIPTION |
|  | preferred-read-replica | The current read replica for the partition, or -1 if reading from leader |
|  | records-lag | The latest lag of the partition |
|  | records-lag-avg | The average lag of the partition |
|  | records-lag-max | The max lag of the partition |
|  | records-lead | The latest lead of the partition |
|  | records-lead-avg | The average lead of the partition |
|  | records-lead-min | The min lead of the partition |

#### [Connect Monitoring](#connect_monitoring)

A Connect worker process contains all the producer and consumer metrics
as well as metrics specific to Connect. The worker process itself has a
number of metrics, while each connector and task have additional
metrics. \[2021-04-14 09:34:28,197\] INFO Metrics scheduler closed
(org.apache.kafka.common.metrics.Metrics:659) \[2021-04-14
09:34:28,199\] INFO Metrics reporters closed
(org.apache.kafka.common.metrics.Metrics:669)

| kafka.connect:type=connect-worker-metrics |  |  |
| --- | --- | --- |
|  | ATTRIBUTE NAME | DESCRIPTION |
|  | connector-count | The number of connectors run in this worker. |
|  | connector-startup-attempts-total | The total number of connector startups that this worker has attempted. |
|  | connector-startup-failure-percentage | The average percentage of this worker's connectors starts that failed. |
|  | connector-startup-failure-total | The total number of connector starts that failed. |
|  | connector-startup-success-percentage | The average percentage of this worker's connectors starts that succeeded. |
|  | connector-startup-success-total | The total number of connector starts that succeeded. |
|  | task-count | The number of tasks run in this worker. |
|  | task-startup-attempts-total | The total number of task startups that this worker has attempted. |
|  | task-startup-failure-percentage | The average percentage of this worker's tasks starts that failed. |
|  | task-startup-failure-total | The total number of task starts that failed. |
|  | task-startup-success-percentage | The average percentage of this worker's tasks starts that succeeded. |
|  | task-startup-success-total | The total number of task starts that succeeded. |
| kafka.connect:type=connect-worker-metrics,connector="{connector}" |  |  |
|  | ATTRIBUTE NAME | DESCRIPTION |
|  | connector-destroyed-task-count | The number of destroyed tasks of the connector on the worker. |
|  | connector-failed-task-count | The number of failed tasks of the connector on the worker. |
|  | connector-paused-task-count | The number of paused tasks of the connector on the worker. |
|  | connector-running-task-count | The number of running tasks of the connector on the worker. |
|  | connector-total-task-count | The number of tasks of the connector on the worker. |
|  | connector-unassigned-task-count | The number of unassigned tasks of the connector on the worker. |
| kafka.connect:type=connect-worker-rebalance-metrics |  |  |
|  | ATTRIBUTE NAME | DESCRIPTION |
|  | completed-rebalances-total | The total number of rebalances completed by this worker. |
|  | connect-protocol | The Connect protocol used by this cluster |
|  | epoch | The epoch or generation number of this worker. |
|  | leader-name | The name of the group leader. |
|  | rebalance-avg-time-ms | The average time in milliseconds spent by this worker to rebalance. |
|  | rebalance-max-time-ms | The maximum time in milliseconds spent by this worker to rebalance. |
|  | rebalancing | Whether this worker is currently rebalancing. |
|  | time-since-last-rebalance-ms | The time in milliseconds since this worker completed the most recent rebalance. |
| kafka.connect:type=connector-metrics,connector="{connector}" |  |  |
|  | ATTRIBUTE NAME | DESCRIPTION |
|  | connector-class | The name of the connector class. |
|  | connector-type | The type of the connector. One of 'source' or 'sink'. |
|  | connector-version | The version of the connector class, as reported by the connector. |
|  | status | The status of the connector. One of 'unassigned', 'running', 'paused', 'failed', or 'destroyed'. |
| kafka.connect:type=connector-task-metrics,connector="{connector}",task="{task}" |  |  |
|  | ATTRIBUTE NAME | DESCRIPTION |
|  | batch-size-avg | The average size of the batches processed by the connector. |
|  | batch-size-max | The maximum size of the batches processed by the connector. |
|  | offset-commit-avg-time-ms | The average time in milliseconds taken by this task to commit offsets. |
|  | offset-commit-failure-percentage | The average percentage of this task's offset commit attempts that failed. |
|  | offset-commit-max-time-ms | The maximum time in milliseconds taken by this task to commit offsets. |
|  | offset-commit-success-percentage | The average percentage of this task's offset commit attempts that succeeded. |
|  | pause-ratio | The fraction of time this task has spent in the pause state. |
|  | running-ratio | The fraction of time this task has spent in the running state. |
|  | status | The status of the connector task. One of 'unassigned', 'running', 'paused', 'failed', or 'destroyed'. |
| kafka.connect:type=sink-task-metrics,connector="{connector}",task="{task}" |  |  |
|  | ATTRIBUTE NAME | DESCRIPTION |
|  | offset-commit-completion-rate | The average per-second number of offset commit completions that were completed successfully. |
|  | offset-commit-completion-total | The total number of offset commit completions that were completed successfully. |
|  | offset-commit-seq-no | The current sequence number for offset commits. |
|  | offset-commit-skip-rate | The average per-second number of offset commit completions that were received too late and skipped/ignored. |
|  | offset-commit-skip-total | The total number of offset commit completions that were received too late and skipped/ignored. |
|  | partition-count | The number of topic partitions assigned to this task belonging to the named sink connector in this worker. |
|  | put-batch-avg-time-ms | The average time taken by this task to put a batch of sinks records. |
|  | put-batch-max-time-ms | The maximum time taken by this task to put a batch of sinks records. |
|  | sink-record-active-count | The number of records that have been read from Kafka but not yet completely committed/flushed/acknowledged by the sink task. |
|  | sink-record-active-count-avg | The average number of records that have been read from Kafka but not yet completely committed/flushed/acknowledged by the sink task. |
|  | sink-record-active-count-max | The maximum number of records that have been read from Kafka but not yet completely committed/flushed/acknowledged by the sink task. |
|  | sink-record-lag-max | The maximum lag in terms of number of records that the sink task is behind the consumer's position for any topic partitions. |
|  | sink-record-read-rate | The average per-second number of records read from Kafka for this task belonging to the named sink connector in this worker. This is before transformations are applied. |
|  | sink-record-read-total | The total number of records read from Kafka by this task belonging to the named sink connector in this worker, since the task was last restarted. |
|  | sink-record-send-rate | The average per-second number of records output from the transformations and sent/put to this task belonging to the named sink connector in this worker. This is after transformations are applied and excludes any records filtered out by the transformations. |
|  | sink-record-send-total | The total number of records output from the transformations and sent/put to this task belonging to the named sink connector in this worker, since the task was last restarted. |
| kafka.connect:type=source-task-metrics,connector="{connector}",task="{task}" |  |  |
|  | ATTRIBUTE NAME | DESCRIPTION |
|  | poll-batch-avg-time-ms | The average time in milliseconds taken by this task to poll for a batch of source records. |
|  | poll-batch-max-time-ms | The maximum time in milliseconds taken by this task to poll for a batch of source records. |
|  | source-record-active-count | The number of records that have been produced by this task but not yet completely written to Kafka. |
|  | source-record-active-count-avg | The average number of records that have been produced by this task but not yet completely written to Kafka. |
|  | source-record-active-count-max | The maximum number of records that have been produced by this task but not yet completely written to Kafka. |
|  | source-record-poll-rate | The average per-second number of records produced/polled (before transformation) by this task belonging to the named source connector in this worker. |
|  | source-record-poll-total | The total number of records produced/polled (before transformation) by this task belonging to the named source connector in this worker. |
|  | source-record-write-rate | The average per-second number of records output from the transformations and written to Kafka for this task belonging to the named source connector in this worker. This is after transformations are applied and excludes any records filtered out by the transformations. |
|  | source-record-write-total | The number of records output from the transformations and written to Kafka for this task belonging to the named source connector in this worker, since the task was last restarted. |
| kafka.connect:type=task-error-metrics,connector="{connector}",task="{task}" |  |  |
|  | ATTRIBUTE NAME | DESCRIPTION |
|  | deadletterqueue-produce-failures | The number of failed writes to the dead letter queue. |
|  | deadletterqueue-produce-requests | The number of attempted writes to the dead letter queue. |
|  | last-error-timestamp | The epoch timestamp when this task last encountered an error. |
|  | total-errors-logged | The number of errors that were logged. |
|  | total-record-errors | The number of record processing errors in this task. |
|  | total-record-failures | The number of record processing failures in this task. |
|  | total-records-skipped | The number of records skipped due to errors. |
|  | total-retries | The number of operations retried. |

#### [Streams Monitoring](#kafka_streams_monitoring)

A Kafka Streams instance contains all the producer and consumer metrics
as well as additional metrics specific to Streams. By default Kafka
Streams has metrics with three recording levels: `info`, `debug`, and
`trace`.

Note that the metrics have a 4-layer hierarchy. At the top level there
are client-level metrics for each started Kafka Streams client. Each
client has stream threads, with their own metrics. Each stream thread
has tasks, with their own metrics. Each task has a number of processor
nodes, with their own metrics. Each task also has a number of state
stores and record caches, all with their own metrics.

Use the following configuration option to specify which metrics you want
collected:

    metrics.recording.level="info"

##### [Client Metrics](#kafka_streams_client_monitoring)

All of the following metrics have a recording level of `info`:

| Metric/Attribute name | Description                                                                      | Mbean name                                               |
| --------------------- | -------------------------------------------------------------------------------- | -------------------------------------------------------- |
| version               | The version of the Kafka Streams client.                                         | kafka.streams:type=stream-metrics,client-id=(\[-.\\w\]+) |
| commit-id             | The version control commit ID of the Kafka Streams client.                       | kafka.streams:type=stream-metrics,client-id=(\[-.\\w\]+) |
| application-id        | The application ID of the Kafka Streams client.                                  | kafka.streams:type=stream-metrics,client-id=(\[-.\\w\]+) |
| topology-description  | The description of the topology executed in the Kafka Streams client.            | kafka.streams:type=stream-metrics,client-id=(\[-.\\w\]+) |
| state                 | The state of the Kafka Streams client.                                           | kafka.streams:type=stream-metrics,client-id=(\[-.\\w\]+) |
| failed-stream-threads | The number of failed stream threads since the start of the Kafka Streams client. | kafka.streams:type=stream-metrics,client-id=(\[-.\\w\]+) |

##### [Thread Metrics](#kafka_streams_thread_monitoring)

All of the following metrics have a recording level of `info`:

| Metric/Attribute name | Description                                                                                | Mbean name                                                      |
| --------------------- | ------------------------------------------------------------------------------------------ | --------------------------------------------------------------- |
| commit-latency-avg    | The average execution time in ms, for committing, across all running tasks of this thread. | kafka.streams:type=stream-thread-metrics,thread-id=(\[-.\\w\]+) |
| commit-latency-max    | The maximum execution time in ms, for committing, across all running tasks of this thread. | kafka.streams:type=stream-thread-metrics,thread-id=(\[-.\\w\]+) |
| poll-latency-avg      | The average execution time in ms, for consumer polling.                                    | kafka.streams:type=stream-thread-metrics,thread-id=(\[-.\\w\]+) |
| poll-latency-max      | The maximum execution time in ms, for consumer polling.                                    | kafka.streams:type=stream-thread-metrics,thread-id=(\[-.\\w\]+) |
| process-latency-avg   | The average execution time in ms, for processing.                                          | kafka.streams:type=stream-thread-metrics,thread-id=(\[-.\\w\]+) |
| process-latency-max   | The maximum execution time in ms, for processing.                                          | kafka.streams:type=stream-thread-metrics,thread-id=(\[-.\\w\]+) |
| punctuate-latency-avg | The average execution time in ms, for punctuating.                                         | kafka.streams:type=stream-thread-metrics,thread-id=(\[-.\\w\]+) |
| punctuate-latency-max | The maximum execution time in ms, for punctuating.                                         | kafka.streams:type=stream-thread-metrics,thread-id=(\[-.\\w\]+) |
| commit-rate           | The average number of commits per second.                                                  | kafka.streams:type=stream-thread-metrics,thread-id=(\[-.\\w\]+) |
| commit-total          | The total number of commit calls.                                                          | kafka.streams:type=stream-thread-metrics,thread-id=(\[-.\\w\]+) |
| poll-rate             | The average number of consumer poll calls per second.                                      | kafka.streams:type=stream-thread-metrics,thread-id=(\[-.\\w\]+) |
| poll-total            | The total number of consumer poll calls.                                                   | kafka.streams:type=stream-thread-metrics,thread-id=(\[-.\\w\]+) |
| process-rate          | The average number of processed records per second.                                        | kafka.streams:type=stream-thread-metrics,thread-id=(\[-.\\w\]+) |
| process-total         | The total number of processed records.                                                     | kafka.streams:type=stream-thread-metrics,thread-id=(\[-.\\w\]+) |
| punctuate-rate        | The average number of punctuate calls per second.                                          | kafka.streams:type=stream-thread-metrics,thread-id=(\[-.\\w\]+) |
| punctuate-total       | The total number of punctuate calls.                                                       | kafka.streams:type=stream-thread-metrics,thread-id=(\[-.\\w\]+) |
| task-created-rate     | The average number of tasks created per second.                                            | kafka.streams:type=stream-thread-metrics,thread-id=(\[-.\\w\]+) |
| task-created-total    | The total number of tasks created.                                                         | kafka.streams:type=stream-thread-metrics,thread-id=(\[-.\\w\]+) |
| task-closed-rate      | The average number of tasks closed per second.                                             | kafka.streams:type=stream-thread-metrics,thread-id=(\[-.\\w\]+) |
| task-closed-total     | The total number of tasks closed.                                                          | kafka.streams:type=stream-thread-metrics,thread-id=(\[-.\\w\]+) |

##### [Task Metrics](#kafka_streams_task_monitoring)

All of the following metrics have a recording level of `debug`, except
for the dropped-records-\*, active-process-ratio, and
record-e2e-latency-\* metrics which have a recording level of `info`:

| Metric/Attribute name     | Description                                                                                                                                               | Mbean name                                                                         |
| ------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------- |
| process-latency-avg       | The average execution time in ns, for processing.                                                                                                         | kafka.streams:type=stream-task-metrics,thread-id=(\[-.\\w\]+),task-id=(\[-.\\w\]+) |
| process-latency-max       | The maximum execution time in ns, for processing.                                                                                                         | kafka.streams:type=stream-task-metrics,thread-id=(\[-.\\w\]+),task-id=(\[-.\\w\]+) |
| process-rate              | The average number of processed records per second across all source processor nodes of this task.                                                        | kafka.streams:type=stream-task-metrics,thread-id=(\[-.\\w\]+),task-id=(\[-.\\w\]+) |
| process-total             | The total number of processed records across all source processor nodes of this task.                                                                     | kafka.streams:type=stream-task-metrics,thread-id=(\[-.\\w\]+),task-id=(\[-.\\w\]+) |
| commit-latency-avg        | The average execution time in ns, for committing.                                                                                                         | kafka.streams:type=stream-task-metrics,thread-id=(\[-.\\w\]+),task-id=(\[-.\\w\]+) |
| commit-latency-max        | The maximum execution time in ns, for committing.                                                                                                         | kafka.streams:type=stream-task-metrics,thread-id=(\[-.\\w\]+),task-id=(\[-.\\w\]+) |
| commit-rate               | The average number of commit calls per second.                                                                                                            | kafka.streams:type=stream-task-metrics,thread-id=(\[-.\\w\]+),task-id=(\[-.\\w\]+) |
| commit-total              | The total number of commit calls.                                                                                                                         | kafka.streams:type=stream-task-metrics,thread-id=(\[-.\\w\]+),task-id=(\[-.\\w\]+) |
| record-lateness-avg       | The average observed lateness of records (stream time - record timestamp).                                                                                | kafka.streams:type=stream-task-metrics,thread-id=(\[-.\\w\]+),task-id=(\[-.\\w\]+) |
| record-lateness-max       | The max observed lateness of records (stream time - record timestamp).                                                                                    | kafka.streams:type=stream-task-metrics,thread-id=(\[-.\\w\]+),task-id=(\[-.\\w\]+) |
| enforced-processing-rate  | The average number of enforced processings per second.                                                                                                    | kafka.streams:type=stream-task-metrics,thread-id=(\[-.\\w\]+),task-id=(\[-.\\w\]+) |
| enforced-processing-total | The total number enforced processings.                                                                                                                    | kafka.streams:type=stream-task-metrics,thread-id=(\[-.\\w\]+),task-id=(\[-.\\w\]+) |
| dropped-records-rate      | The average number of records dropped within this task.                                                                                                   | kafka.streams:type=stream-task-metrics,thread-id=(\[-.\\w\]+),task-id=(\[-.\\w\]+) |
| dropped-records-total     | The total number of records dropped within this task.                                                                                                     | kafka.streams:type=stream-task-metrics,thread-id=(\[-.\\w\]+),task-id=(\[-.\\w\]+) |
| active-process-ratio      | The total number of records dropped within this task.                                                                                                     | kafka.streams:type=stream-task-metrics,thread-id=(\[-.\\w\]+),task-id=(\[-.\\w\]+) |
| record-e2e-latency-avg    | The average end-to-end latency of a record, measured by comparing the record timestamp with the system time when it has been fully processed by the node. | kafka.streams:type=stream-task-metrics,thread-id=(\[-.\\w\]+),task-id=(\[-.\\w\]+) |
| record-e2e-latency-max    | The maximum end-to-end latency of a record, measured by comparing the record timestamp with the system time when it has been fully processed by the node. | kafka.streams:type=stream-task-metrics,thread-id=(\[-.\\w\]+),task-id=(\[-.\\w\]+) |
| record-e2e-latency-min    | The minimum end-to-end latency of a record, measured by comparing the record timestamp with the system time when it has been fully processed by the node. | kafka.streams:type=stream-task-metrics,thread-id=(\[-.\\w\]+),task-id=(\[-.\\w\]+) |

##### [Processor Node Metrics](#kafka_streams_node_monitoring)

The following metrics are only available on certain types of nodes,
i.e., process-rate and process-total are only available for source
processor nodes and suppression-emit-rate and suppression-emit-total are
only available for suppression operation nodes. All of the metrics have
a recording level of `debug`:

| Metric/Attribute name  | Description                                                                                     | Mbean name                                                                                                                  |
| ---------------------- | ----------------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------- |
| process-rate           | The average number of records processed by a source processor node per second.                  | kafka.streams:type=stream-processor-node-metrics,thread-id=(\[-.\\w\]+),task-id=(\[-.\\w\]+),processor-node-id=(\[-.\\w\]+) |
| process-total          | The total number of records processed by a source processor node per second.                    | kafka.streams:type=stream-processor-node-metrics,thread-id=(\[-.\\w\]+),task-id=(\[-.\\w\]+),processor-node-id=(\[-.\\w\]+) |
| suppression-emit-rate  | The rate at which records that have been emitted downstream from suppression operation nodes.   | kafka.streams:type=stream-processor-node-metrics,thread-id=(\[-.\\w\]+),task-id=(\[-.\\w\]+),processor-node-id=(\[-.\\w\]+) |
| suppression-emit-total | The total number of records that have been emitted downstream from suppression operation nodes. | kafka.streams:type=stream-processor-node-metrics,thread-id=(\[-.\\w\]+),task-id=(\[-.\\w\]+),processor-node-id=(\[-.\\w\]+) |

##### [State Store Metrics](#kafka_streams_store_monitoring)

All of the following metrics have a recording level of `debug`, except
for the record-e2e-latency-\* metrics which have a recording level
`trace>`. Note that the `store-scope` value is specified in
`StoreSupplier#metricsScope()` for user's customized state stores; for
built-in state stores, currently we have:

  - `in-memory-state`
  - `in-memory-lru-state`
  - `in-memory-window-state`
  - `in-memory-suppression` (for suppression buffers)
  - `rocksdb-state` (for RocksDB backed key-value store)
  - `rocksdb-window-state` (for RocksDB backed window store)
  - `rocksdb-session-state` (for RocksDB backed session store)

Metrics suppression-buffer-size-avg, suppression-buffer-size-max,
suppression-buffer-count-avg, and suppression-buffer-count-max are only
available for suppression buffers. All other metrics are not available
for suppression buffers.

| Metric/Attribute name        | Description                                                                                                                                               | Mbean name                                                                                                                |
| ---------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------- |
| put-latency-avg              | The average put execution time in ns.                                                                                                                     | kafka.streams:type=stream-state-metrics,thread-id=(\[-.\\w\]+),task-id=(\[-.\\w\]+),\[store-scope\]-id=(\[-.\\w\]+)       |
| put-latency-max              | The maximum put execution time in ns.                                                                                                                     | kafka.streams:type=stream-state-metrics,thread-id=(\[-.\\w\]+),task-id=(\[-.\\w\]+),\[store-scope\]-id=(\[-.\\w\]+)       |
| put-if-absent-latency-avg    | The average put-if-absent execution time in ns.                                                                                                           | kafka.streams:type=stream-state-metrics,thread-id=(\[-.\\w\]+),task-id=(\[-.\\w\]+),\[store-scope\]-id=(\[-.\\w\]+)       |
| put-if-absent-latency-max    | The maximum put-if-absent execution time in ns.                                                                                                           | kafka.streams:type=stream-state-metrics,thread-id=(\[-.\\w\]+),task-id=(\[-.\\w\]+),\[store-scope\]-id=(\[-.\\w\]+)       |
| get-latency-avg              | The average get execution time in ns.                                                                                                                     | kafka.streams:type=stream-state-metrics,thread-id=(\[-.\\w\]+),task-id=(\[-.\\w\]+),\[store-scope\]-id=(\[-.\\w\]+)       |
| get-latency-max              | The maximum get execution time in ns.                                                                                                                     | kafka.streams:type=stream-state-metrics,thread-id=(\[-.\\w\]+),task-id=(\[-.\\w\]+),\[store-scope\]-id=(\[-.\\w\]+)       |
| delete-latency-avg           | The average delete execution time in ns.                                                                                                                  | kafka.streams:type=stream-state-metrics,thread-id=(\[-.\\w\]+),task-id=(\[-.\\w\]+),\[store-scope\]-id=(\[-.\\w\]+)       |
| delete-latency-max           | The maximum delete execution time in ns.                                                                                                                  | kafka.streams:type=stream-state-metrics,thread-id=(\[-.\\w\]+),task-id=(\[-.\\w\]+),\[store-scope\]-id=(\[-.\\w\]+)       |
| put-all-latency-avg          | The average put-all execution time in ns.                                                                                                                 | kafka.streams:type=stream-state-metrics,thread-id=(\[-.\\w\]+),task-id=(\[-.\\w\]+),\[store-scope\]-id=(\[-.\\w\]+)       |
| put-all-latency-max          | The maximum put-all execution time in ns.                                                                                                                 | kafka.streams:type=stream-state-metrics,thread-id=(\[-.\\w\]+),task-id=(\[-.\\w\]+),\[store-scope\]-id=(\[-.\\w\]+)       |
| all-latency-avg              | The average all operation execution time in ns.                                                                                                           | kafka.streams:type=stream-state-metrics,thread-id=(\[-.\\w\]+),task-id=(\[-.\\w\]+),\[store-scope\]-id=(\[-.\\w\]+)       |
| all-latency-max              | The maximum all operation execution time in ns.                                                                                                           | kafka.streams:type=stream-state-metrics,thread-id=(\[-.\\w\]+),task-id=(\[-.\\w\]+),\[store-scope\]-id=(\[-.\\w\]+)       |
| range-latency-avg            | The average range execution time in ns.                                                                                                                   | kafka.streams:type=stream-state-metrics,thread-id=(\[-.\\w\]+),task-id=(\[-.\\w\]+),\[store-scope\]-id=(\[-.\\w\]+)       |
| range-latency-max            | The maximum range execution time in ns.                                                                                                                   | kafka.streams:type=stream-state-metrics,thread-id=(\[-.\\w\]+),task-id=(\[-.\\w\]+),\[store-scope\]-id=(\[-.\\w\]+)       |
| flush-latency-avg            | The average flush execution time in ns.                                                                                                                   | kafka.streams:type=stream-state-metrics,thread-id=(\[-.\\w\]+),task-id=(\[-.\\w\]+),\[store-scope\]-id=(\[-.\\w\]+)       |
| flush-latency-max            | The maximum flush execution time in ns.                                                                                                                   | kafka.streams:type=stream-state-metrics,thread-id=(\[-.\\w\]+),task-id=(\[-.\\w\]+),\[store-scope\]-id=(\[-.\\w\]+)       |
| restore-latency-avg          | The average restore execution time in ns.                                                                                                                 | kafka.streams:type=stream-state-metrics,thread-id=(\[-.\\w\]+),task-id=(\[-.\\w\]+),\[store-scope\]-id=(\[-.\\w\]+)       |
| restore-latency-max          | The maximum restore execution time in ns.                                                                                                                 | kafka.streams:type=stream-state-metrics,thread-id=(\[-.\\w\]+),task-id=(\[-.\\w\]+),\[store-scope\]-id=(\[-.\\w\]+)       |
| put-rate                     | The average put rate for this store.                                                                                                                      | kafka.streams:type=stream-state-metrics,thread-id=(\[-.\\w\]+),task-id=(\[-.\\w\]+),\[store-scope\]-id=(\[-.\\w\]+)       |
| put-if-absent-rate           | The average put-if-absent rate for this store.                                                                                                            | kafka.streams:type=stream-state-metrics,thread-id=(\[-.\\w\]+),task-id=(\[-.\\w\]+),\[store-scope\]-id=(\[-.\\w\]+)       |
| get-rate                     | The average get rate for this store.                                                                                                                      | kafka.streams:type=stream-state-metrics,thread-id=(\[-.\\w\]+),task-id=(\[-.\\w\]+),\[store-scope\]-id=(\[-.\\w\]+)       |
| delete-rate                  | The average delete rate for this store.                                                                                                                   | kafka.streams:type=stream-state-metrics,thread-id=(\[-.\\w\]+),task-id=(\[-.\\w\]+),\[store-scope\]-id=(\[-.\\w\]+)       |
| put-all-rate                 | The average put-all rate for this store.                                                                                                                  | kafka.streams:type=stream-state-metrics,thread-id=(\[-.\\w\]+),task-id=(\[-.\\w\]+),\[store-scope\]-id=(\[-.\\w\]+)       |
| all-rate                     | The average all operation rate for this store.                                                                                                            | kafka.streams:type=stream-state-metrics,thread-id=(\[-.\\w\]+),task-id=(\[-.\\w\]+),\[store-scope\]-id=(\[-.\\w\]+)       |
| range-rate                   | The average range rate for this store.                                                                                                                    | kafka.streams:type=stream-state-metrics,thread-id=(\[-.\\w\]+),task-id=(\[-.\\w\]+),\[store-scope\]-id=(\[-.\\w\]+)       |
| flush-rate                   | The average flush rate for this store.                                                                                                                    | kafka.streams:type=stream-state-metrics,thread-id=(\[-.\\w\]+),task-id=(\[-.\\w\]+),\[store-scope\]-id=(\[-.\\w\]+)       |
| restore-rate                 | The average restore rate for this store.                                                                                                                  | kafka.streams:type=stream-state-metrics,thread-id=(\[-.\\w\]+),task-id=(\[-.\\w\]+),\[store-scope\]-id=(\[-.\\w\]+)       |
| suppression-buffer-size-avg  | The average total size, in bytes, of the buffered data over the sampling window.                                                                          | kafka.streams:type=stream-state-metrics,thread-id=(\[-.\\w\]+),task-id=(\[-.\\w\]+),in-memory-suppression-id=(\[-.\\w\]+) |
| suppression-buffer-size-max  | The maximum total size, in bytes, of the buffered data over the sampling window.                                                                          | kafka.streams:type=stream-state-metrics,thread-id=(\[-.\\w\]+),task-id=(\[-.\\w\]+),in-memory-suppression-id=(\[-.\\w\]+) |
| suppression-buffer-count-avg | The average number of records buffered over the sampling window.                                                                                          | kafka.streams:type=stream-state-metrics,thread-id=(\[-.\\w\]+),task-id=(\[-.\\w\]+),in-memory-suppression-id=(\[-.\\w\]+) |
| suppression-buffer-count-max | The maximum number of records buffered over the sampling window.                                                                                          | kafka.streams:type=stream-state-metrics,thread-id=(\[-.\\w\]+),task-id=(\[-.\\w\]+),in-memory-suppression-id=(\[-.\\w\]+) |
| record-e2e-latency-avg       | The average end-to-end latency of a record, measured by comparing the record timestamp with the system time when it has been fully processed by the node. | kafka.streams:type=stream-state-metrics,thread-id=(\[-.\\w\]+),task-id=(\[-.\\w\]+),\[store-scope\]-id=(\[-.\\w\]+)       |
| record-e2e-latency-max       | The maximum end-to-end latency of a record, measured by comparing the record timestamp with the system time when it has been fully processed by the node. | kafka.streams:type=stream-state-metrics,thread-id=(\[-.\\w\]+),task-id=(\[-.\\w\]+),\[store-scope\]-id=(\[-.\\w\]+)       |
| record-e2e-latency-min       | The minimum end-to-end latency of a record, measured by comparing the record timestamp with the system time when it has been fully processed by the node. | kafka.streams:type=stream-state-metrics,thread-id=(\[-.\\w\]+),task-id=(\[-.\\w\]+),\[store-scope\]-id=(\[-.\\w\]+)       |

##### [RocksDB Metrics](#kafka_streams_rocksdb_monitoring)

RocksDB metrics are grouped into statistics-based metrics and
properties-based metrics. The former are recorded from statistics that a
RocksDB state store collects whereas the latter are recorded from
properties that RocksDB exposes. Statistics collected by RocksDB provide
cumulative measurements over time, e.g. bytes written to the state
store. Properties exposed by RocksDB provide current measurements, e.g.,
the amount of memory currently used. Note that the `store-scope` for
built-in RocksDB state stores are currently the following:

  - `rocksdb-state` (for RocksDB backed key-value store)
  - `rocksdb-window-state` (for RocksDB backed window store)
  - `rocksdb-session-state` (for RocksDB backed session store)

**RocksDB Statistics-based Metrics:** All of the following
statistics-based metrics have a recording level of `debug` because
collecting statistics in [RocksDB may have an impact on
performance](https://github.com/facebook/rocksdb/wiki/Statistics#stats-level-and-performance-costs).
Statistics-based metrics are collected every minute from the RocksDB
state stores. If a state store consists of multiple RocksDB instances,
as is the case for WindowStores and SessionStores, each metric reports
an aggregation over the RocksDB instances of the state store.

| Metric/Attribute name         | Description                                                                                                   | Mbean name                                                                                                          |
| ----------------------------- | ------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------- |
| bytes-written-rate            | The average number of bytes written per second to the RocksDB state store.                                    | kafka.streams:type=stream-state-metrics,thread-id=(\[-.\\w\]+),task-id=(\[-.\\w\]+),\[store-scope\]-id=(\[-.\\w\]+) |
| bytes-written-total           | The total number of bytes written to the RocksDB state store.                                                 | kafka.streams:type=stream-state-metrics,thread-id=(\[-.\\w\]+),task-id=(\[-.\\w\]+),\[store-scope\]-id=(\[-.\\w\]+) |
| bytes-read-rate               | The average number of bytes read per second from the RocksDB state store.                                     | kafka.streams:type=stream-state-metrics,thread-id=(\[-.\\w\]+),task-id=(\[-.\\w\]+),\[store-scope\]-id=(\[-.\\w\]+) |
| bytes-read-total              | The total number of bytes read from the RocksDB state store.                                                  | kafka.streams:type=stream-state-metrics,thread-id=(\[-.\\w\]+),task-id=(\[-.\\w\]+),\[store-scope\]-id=(\[-.\\w\]+) |
| memtable-bytes-flushed-rate   | The average number of bytes flushed per second from the memtable to disk.                                     | kafka.streams:type=stream-state-metrics,thread-id=(\[-.\\w\]+),task-id=(\[-.\\w\]+),\[store-scope\]-id=(\[-.\\w\]+) |
| memtable-bytes-flushed-total  | The total number of bytes flushed from the memtable to disk.                                                  | kafka.streams:type=stream-state-metrics,thread-id=(\[-.\\w\]+),task-id=(\[-.\\w\]+),\[store-scope\]-id=(\[-.\\w\]+) |
| memtable-hit-ratio            | The ratio of memtable hits relative to all lookups to the memtable.                                           | kafka.streams:type=stream-state-metrics,thread-id=(\[-.\\w\]+),task-id=(\[-.\\w\]+),\[store-scope\]-id=(\[-.\\w\]+) |
| block-cache-data-hit-ratio    | The ratio of block cache hits for data blocks relative to all lookups for data blocks to the block cache.     | kafka.streams:type=stream-state-metrics,thread-id=(\[-.\\w\]+),task-id=(\[-.\\w\]+),\[store-scope\]-id=(\[-.\\w\]+) |
| block-cache-index-hit-ratio   | The ratio of block cache hits for index blocks relative to all lookups for index blocks to the block cache.   | kafka.streams:type=stream-state-metrics,thread-id=(\[-.\\w\]+),task-id=(\[-.\\w\]+),\[store-scope\]-id=(\[-.\\w\]+) |
| block-cache-filter-hit-ratio  | The ratio of block cache hits for filter blocks relative to all lookups for filter blocks to the block cache. | kafka.streams:type=stream-state-metrics,thread-id=(\[-.\\w\]+),task-id=(\[-.\\w\]+),\[store-scope\]-id=(\[-.\\w\]+) |
| write-stall-duration-avg      | The average duration of write stalls in ms.                                                                   | kafka.streams:type=stream-state-metrics,thread-id=(\[-.\\w\]+),task-id=(\[-.\\w\]+),\[store-scope\]-id=(\[-.\\w\]+) |
| write-stall-duration-total    | The total duration of write stalls in ms.                                                                     | kafka.streams:type=stream-state-metrics,thread-id=(\[-.\\w\]+),task-id=(\[-.\\w\]+),\[store-scope\]-id=(\[-.\\w\]+) |
| bytes-read-compaction-rate    | The average number of bytes read per second during compaction.                                                | kafka.streams:type=stream-state-metrics,thread-id=(\[-.\\w\]+),task-id=(\[-.\\w\]+),\[store-scope\]-id=(\[-.\\w\]+) |
| bytes-written-compaction-rate | The average number of bytes written per second during compaction.                                             | kafka.streams:type=stream-state-metrics,thread-id=(\[-.\\w\]+),task-id=(\[-.\\w\]+),\[store-scope\]-id=(\[-.\\w\]+) |
| number-open-files             | The number of current open files.                                                                             | kafka.streams:type=stream-state-metrics,thread-id=(\[-.\\w\]+),task-id=(\[-.\\w\]+),\[store-scope\]-id=(\[-.\\w\]+) |
| number-file-errors-total      | The total number of file errors occurred.                                                                     | kafka.streams:type=stream-state-metrics,thread-id=(\[-.\\w\]+),task-id=(\[-.\\w\]+),\[store-scope\]-id=(\[-.\\w\]+) |

**RocksDB Properties-based Metrics:** All of the following
properties-based metrics have a recording level of `info` and are
recorded when the metrics are accessed. If a state store consists of
multiple RocksDB instances, as is the case for WindowStores and
SessionStores, each metric reports the sum over all the RocksDB
instances of the state store, except for the block cache metrics
`block-cache-*`. The block cache metrics report the sum over all RocksDB
instances if each instance uses its own block cache, and they report the
recorded value from only one instance if a single block cache is shared
among all instances.

| Metric/Attribute name             | Description                                                                                                                                              | Mbean name                                                                                                          |
| --------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------- |
| num-immutable-mem-table           | The number of immutable memtables that have not yet been flushed.                                                                                        | kafka.streams:type=stream-state-metrics,thread-id=(\[-.\\w\]+),task-id=(\[-.\\w\]+),\[store-scope\]-id=(\[-.\\w\]+) |
| cur-size-active-mem-table         | The approximate size of the active memtable in bytes.                                                                                                    | kafka.streams:type=stream-state-metrics,thread-id=(\[-.\\w\]+),task-id=(\[-.\\w\]+),\[store-scope\]-id=(\[-.\\w\]+) |
| cur-size-all-mem-tables           | The approximate size of active and unflushed immutable memtables in bytes.                                                                               | kafka.streams:type=stream-state-metrics,thread-id=(\[-.\\w\]+),task-id=(\[-.\\w\]+),\[store-scope\]-id=(\[-.\\w\]+) |
| size-all-mem-tables               | The approximate size of active, unflushed immutable, and pinned immutable memtables in bytes.                                                            | kafka.streams:type=stream-state-metrics,thread-id=(\[-.\\w\]+),task-id=(\[-.\\w\]+),\[store-scope\]-id=(\[-.\\w\]+) |
| num-entries-active-mem-table      | The number of entries in the active memtable.                                                                                                            | kafka.streams:type=stream-state-metrics,thread-id=(\[-.\\w\]+),task-id=(\[-.\\w\]+),\[store-scope\]-id=(\[-.\\w\]+) |
| num-entries-imm-mem-tables        | The number of entries in the unflushed immutable memtables.                                                                                              | kafka.streams:type=stream-state-metrics,thread-id=(\[-.\\w\]+),task-id=(\[-.\\w\]+),\[store-scope\]-id=(\[-.\\w\]+) |
| num-deletes-active-mem-table      | The number of delete entries in the active memtable.                                                                                                     | kafka.streams:type=stream-state-metrics,thread-id=(\[-.\\w\]+),task-id=(\[-.\\w\]+),\[store-scope\]-id=(\[-.\\w\]+) |
| num-deletes-imm-mem-tables        | The number of delete entries in the unflushed immutable memtables.                                                                                       | kafka.streams:type=stream-state-metrics,thread-id=(\[-.\\w\]+),task-id=(\[-.\\w\]+),\[store-scope\]-id=(\[-.\\w\]+) |
| mem-table-flush-pending           | This metric reports 1 if a memtable flush is pending, otherwise it reports 0.                                                                            | kafka.streams:type=stream-state-metrics,thread-id=(\[-.\\w\]+),task-id=(\[-.\\w\]+),\[store-scope\]-id=(\[-.\\w\]+) |
| num-running-flushes               | The number of currently running flushes.                                                                                                                 | kafka.streams:type=stream-state-metrics,thread-id=(\[-.\\w\]+),task-id=(\[-.\\w\]+),\[store-scope\]-id=(\[-.\\w\]+) |
| compaction-pending                | This metric reports 1 if at least one compaction is pending, otherwise it reports 0.                                                                     | kafka.streams:type=stream-state-metrics,thread-id=(\[-.\\w\]+),task-id=(\[-.\\w\]+),\[store-scope\]-id=(\[-.\\w\]+) |
| num-running-compactions           | The number of currently running compactions.                                                                                                             | kafka.streams:type=stream-state-metrics,thread-id=(\[-.\\w\]+),task-id=(\[-.\\w\]+),\[store-scope\]-id=(\[-.\\w\]+) |
| estimate-pending-compaction-bytes | The estimated total number of bytes a compaction needs to rewrite on disk to get all levels down to under target size (only valid for level compaction). | kafka.streams:type=stream-state-metrics,thread-id=(\[-.\\w\]+),task-id=(\[-.\\w\]+),\[store-scope\]-id=(\[-.\\w\]+) |
| total-sst-files-size              | The total size in bytes of all SST files.                                                                                                                | kafka.streams:type=stream-state-metrics,thread-id=(\[-.\\w\]+),task-id=(\[-.\\w\]+),\[store-scope\]-id=(\[-.\\w\]+) |
| live-sst-files-size               | The total size in bytes of all SST files that belong to the latest LSM tree.                                                                             | kafka.streams:type=stream-state-metrics,thread-id=(\[-.\\w\]+),task-id=(\[-.\\w\]+),\[store-scope\]-id=(\[-.\\w\]+) |
| num-live-versions                 | Number of live versions of the LSM tree.                                                                                                                 | kafka.streams:type=stream-state-metrics,thread-id=(\[-.\\w\]+),task-id=(\[-.\\w\]+),\[store-scope\]-id=(\[-.\\w\]+) |
| block-cache-capacity              | The capacity of the block cache in bytes.                                                                                                                | kafka.streams:type=stream-state-metrics,thread-id=(\[-.\\w\]+),task-id=(\[-.\\w\]+),\[store-scope\]-id=(\[-.\\w\]+) |
| block-cache-usage                 | The memory size of the entries residing in block cache in bytes.                                                                                         | kafka.streams:type=stream-state-metrics,thread-id=(\[-.\\w\]+),task-id=(\[-.\\w\]+),\[store-scope\]-id=(\[-.\\w\]+) |
| block-cache-pinned-usage          | The memory size for the entries being pinned in the block cache in bytes.                                                                                | kafka.streams:type=stream-state-metrics,thread-id=(\[-.\\w\]+),task-id=(\[-.\\w\]+),\[store-scope\]-id=(\[-.\\w\]+) |
| estimate-num-keys                 | The estimated number of keys in the active and unflushed immutable memtables and storage.                                                                | kafka.streams:type=stream-state-metrics,thread-id=(\[-.\\w\]+),task-id=(\[-.\\w\]+),\[store-scope\]-id=(\[-.\\w\]+) |
| estimate-table-readers-mem        | The estimated memory in bytes used for reading SST tables, excluding memory used in block cache.                                                         | kafka.streams:type=stream-state-metrics,thread-id=(\[-.\\w\]+),task-id=(\[-.\\w\]+),\[store-scope\]-id=(\[-.\\w\]+) |
| background-errors                 | The total number of background errors.                                                                                                                   | kafka.streams:type=stream-state-metrics,thread-id=(\[-.\\w\]+),task-id=(\[-.\\w\]+),\[store-scope\]-id=(\[-.\\w\]+) |

##### [Record Cache Metrics](#kafka_streams_cache_monitoring)

All of the following metrics have a recording level of `debug`:

| Metric/Attribute name | Description                                                                                             | Mbean name                                                                                                              |
| --------------------- | ------------------------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------- |
| hit-ratio-avg         | The average cache hit ratio defined as the ratio of cache read hits over the total cache read requests. | kafka.streams:type=stream-record-cache-metrics,thread-id=(\[-.\\w\]+),task-id=(\[-.\\w\]+),record-cache-id=(\[-.\\w\]+) |
| hit-ratio-min         | The mininum cache hit ratio.                                                                            | kafka.streams:type=stream-record-cache-metrics,thread-id=(\[-.\\w\]+),task-id=(\[-.\\w\]+),record-cache-id=(\[-.\\w\]+) |
| hit-ratio-max         | The maximum cache hit ratio.                                                                            | kafka.streams:type=stream-record-cache-metrics,thread-id=(\[-.\\w\]+),task-id=(\[-.\\w\]+),record-cache-id=(\[-.\\w\]+) |

#### [Others](#others_monitoring)

We recommend monitoring GC time and other stats and various server stats
such as CPU utilization, I/O service time, etc. On the client side, we
recommend monitoring the message/byte rate (global and per topic),
request rate/size/time, and on the consumer side, max lag in messages
among all partitions and min fetch request rate. For a consumer to keep
up, max lag needs to be less than a threshold and min fetch rate needs
to be larger than 0.

### [6.9 ZooKeeper](#zk)

#### [Stable version](#zkversion)

The current stable branch is 3.5. Kafka is regularly updated to include
the latest release in the 3.5 series.

#### [Operationalizing ZooKeeper](#zkops)

Operationally, we do the following for a healthy ZooKeeper installation:

  - Redundancy in the physical/hardware/network layout: try not to put
    them all in the same rack, decent (but don't go nuts) hardware, try
    to keep redundant power and network paths, etc. A typical ZooKeeper
    ensemble has 5 or 7 servers, which tolerates 2 and 3 servers down,
    respectively. If you have a small deployment, then using 3 servers
    is acceptable, but keep in mind that you'll only be able to tolerate
    1 server down in this case.
  - I/O segregation: if you do a lot of write type traffic you'll almost
    definitely want the transaction logs on a dedicated disk group.
    Writes to the transaction log are synchronous (but batched for
    performance), and consequently, concurrent writes can significantly
    affect performance. ZooKeeper snapshots can be one such a source of
    concurrent writes, and ideally should be written on a disk group
    separate from the transaction log. Snapshots are written to disk
    asynchronously, so it is typically ok to share with the operating
    system and message log files. You can configure a server to use a
    separate disk group with the dataLogDir parameter.
  - Application segregation: Unless you really understand the
    application patterns of other apps that you want to install on the
    same box, it can be a good idea to run ZooKeeper in isolation
    (though this can be a balancing act with the capabilities of the
    hardware).
  - Use care with virtualization: It can work, depending on your cluster
    layout and read/write patterns and SLAs, but the tiny overheads
    introduced by the virtualization layer can add up and throw off
    ZooKeeper, as it can be very time sensitive
  - ZooKeeper configuration: It's java, make sure you give it 'enough'
    heap space (We usually run them with 3-5G, but that's mostly due to
    the data set size we have here). Unfortunately we don't have a good
    formula for it, but keep in mind that allowing for more ZooKeeper
    state means that snapshots can become large, and large snapshots
    affect recovery time. In fact, if the snapshot becomes too large (a
    few gigabytes), then you may need to increase the initLimit
    parameter to give enough time for servers to recover and join the
    ensemble.
  - Monitoring: Both JMX and the 4 letter words (4lw) commands are very
    useful, they do overlap in some cases (and in those cases we prefer
    the 4 letter commands, they seem more predictable, or at the very
    least, they work better with the LI monitoring infrastructure)
  - Don't overbuild the cluster: large clusters, especially in a write
    heavy usage pattern, means a lot of intracluster communication
    (quorums on the writes and subsequent cluster member updates), but
    don't underbuild it (and risk swamping the cluster). Having more
    servers adds to your read capacity.

Overall, we try to keep the ZooKeeper system as small as will handle the
load (plus standard growth capacity planning) and as simple as possible.
We try not to do anything fancy with the configuration or application
layout as compared to the official release as well as keep it as self
contained as possible. For these reasons, we tend to skip the OS
packaged versions, since it has a tendency to try to put things in the
OS standard hierarchy, which can be 'messy', for want of a better way to
word it.

<div class="p-ops">

</div>

## [7. Security](#security)

<div class="p-security">

</div>

## [8. Kafka Connect](#connect)

<div class="p-connect">

</div>

## [9. Kafka Streams](/documentation/streams)

Kafka Streams is a client library for processing and analyzing data
stored in Kafka. It builds upon important stream processing concepts
such as properly distinguishing between event time and processing time,
windowing support, exactly-once processing semantics and simple yet
efficient management of application state.

Kafka Streams has a **low barrier to entry**: You can quickly write and
run a small-scale proof-of-concept on a single machine; and you only
need to run additional instances of your application on multiple
machines to scale up to high-volume production workloads. Kafka Streams
transparently handles the load balancing of multiple instances of the
same application by leveraging Kafka's parallelism model.

Learn More about Kafka Streams read [this](/documentation/streams)
Section.
