
We do:

Graceful shutdown
Leadership
Geo-replication (Rename mirror-maker2)

## [2. APIs](#api)

### APIS
Kafka includes five core apis:
The Producer API allows applications to send streams of data to topics in the Kafka cluster.
The Consumer API allows applications to read streams of data from topics in the Kafka cluster.
The Streams API allows transforming streams of data from input topics to output topics.
The Connect API allows implementing connectors that continually pull from some source system or application into Kafka or push from Kafka into some sink system or application.
The Admin API allows managing and inspecting topics, brokers, and other Kafka objects.
Kafka exposes all its functionality over a language independent protocol which has clients available in many programming languages. However only the Java clients are maintained as part of the main Kafka project, the others are available as independent open source projects. A list of non-Java clients is available here.
#### Producer API
The Producer API allows applications to send streams of data to topics in the Kafka cluster.
Examples showing how to use the producer are given in the javadocs.

To use the producer, you can use the following maven dependency:

		<dependency>
			<groupId>org.apache.kafka</groupId>
			<artifactId>kafka-clients</artifactId>
			<version>2.8.0</version>
		</dependency>
#### Consumer API
The Consumer API allows applications to read streams of data from topics in the Kafka cluster.
Examples showing how to use the consumer are given in the javadocs.

To use the consumer, you can use the following maven dependency:

		<dependency>
			<groupId>org.apache.kafka</groupId>
			<artifactId>kafka-clients</artifactId>
			<version>2.8.0</version>
		</dependency>
#### Streams API
The Streams API allows transforming streams of data from input topics to output topics.
Examples showing how to use this library are given in the javadocs

Additional documentation on using the Streams API is available here.

To use Kafka Streams you can use the following maven dependency:

		<dependency>
			<groupId>org.apache.kafka</groupId>
			<artifactId>kafka-streams</artifactId>
			<version>2.8.0</version>
		</dependency>
When using Scala you may optionally include the kafka-streams-scala library. Additional documentation on using the Kafka Streams DSL for Scala is available in the developer guide.

To use Kafka Streams DSL for Scala for Scala 2.13 you can use the following maven dependency:

		<dependency>
			<groupId>org.apache.kafka</groupId>
			<artifactId>kafka-streams-scala_2.13</artifactId>
			<version>2.8.0</version>
		</dependency>
#### Connect API
The Connect API allows implementing connectors that continually pull from some source data system into Kafka or push from Kafka into some sink data system.
Many users of Connect won't need to use this API directly, though, they can use pre-built connectors without needing to write any code. Additional information on using Connect is available here.

Those who want to implement custom connectors can see the javadoc.

#### Admin API
The Admin API supports managing and inspecting topics, brokers, acls, and other Kafka objects.
To use the Admin API, add the following Maven dependency:

		<dependency>
			<groupId>org.apache.kafka</groupId>
			<artifactId>kafka-clients</artifactId>
			<version>2.8.0</version>
		</dependency>
For more information about the Admin APIs, see the javadoc.

## Configuration

## Design

## Implementation

## Operations

Here is some information on actually running Kafka as a production
system based on usage and experience at LinkedIn. Please send us any
additional tips you know of.

### Basic Kafka Operations

This section will review the most common operations you will perform on
your Kafka cluster. All of the tools reviewed in this section are
available under the `bin/` directory of the Kafka distribution and each
tool will print details on all possible commandline options if it is run
with no arguments.

#### Adding and removing topics

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

#### Modifying topics

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

<!-- 
#### Rack Awareness [Simplify, Q2]

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
-->

#### Mirroring data between clusters & Geo-replication

Kafka administrators can define data flows that cross the boundaries of
individual Kafka clusters, data centers, or geographical regions. Please
refer to the section on [Geo-Replication](#georeplication) for further
information.

#### Checking consumer lag

`rpk cluster offsets`

#### Expanding your cluster [Noah, Michal for May]

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

<!--
##### Automatically migrating data to new machines [Noah, Michal for May]

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

##### Custom partition assignment and migration [Noah, Michal for May]

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
-->

### Multi-Tenancy

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

#### Creating User Spaces (Namespaces) For Tenants With Topic Naming

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

#### Configuring Topics: Data Retention And More

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

#### Securing Clusters and Topics: Authentication, Authorization, Encryption

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

#### Isolating Tenants: Quotas, Rate Limiting, Throttling

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

#### Monitoring and Metering

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

#### Multi-Tenancy and Geo-Replication

Kafka lets you share data across different clusters, which may be
located in different geographical regions, data centers, and so on.
Apart from use cases such as disaster recovery, this functionality is
useful when a multi-tenant setup requires inter-cluster data sharing.
See the section [Geo-Replication (Cross-Cluster Data
Mirroring)](#georeplication) for more information.

#### Further considerations

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
