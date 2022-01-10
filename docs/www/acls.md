---
title: Authorization & authentication
order: 1
---

# Encryption, authorization, and authentication

By default, a Redpanda install starts with no security enabled. Any client can talk over plaintext to Redpanda and perform any operation. You can protect your Redpanda cluster externally through network ACLs, private networks, and so on; this guide does not cover external protection. To ensure defense in depth, it is recommended to always enable encryption and to always use authorization & authentication.

All concepts described here are compatible with Kafka® and its client libraries and CLIs.  

## Prerequisites

- A running Redpanda node.

You can follow our [Getting Started guides](https://vectorized.io/docs/) to learn how to get started with Redpanda.

- TLS certificates.

TLS certificates are needed for encryption. You do not need TLS for testing, but some concepts below rely on TLS certificates. If you have your own certificates, either self-signed or issued by a trusted Certificate Authority, you can use them for this guide. Otherwise, feel free to [use our prepared script](https://gist.github.com/0x5d/56422a0c447e58d8ccbfa0ce1fd6bac6) to generate certificates:

```bash
bash <(curl -s https://gist.githubusercontent.com/0x5d/56422a0c447e58d8ccbfa0ce1fd6bac6/raw/933ca97702f6b844f706b674133105a30bdad3ff/generate-certs.sh)
```

## Encryption: TLS & mTLS

TLS encrypts network traffic between a client and a server, but does not protect against access to a server. With TLS alone (not mTLS), you need to use ACLs to gate access to your brokers.

mTLS, or 2-way TLS, is an authentication method in which the client and the server must both present certificates. This way both can decide whether the other party can be trusted by ensuring they are who they claim to be.

Cloudfare made a very good description of what mTLS is. If you want [you can check their website for more info on that](https://www.cloudflare.com/learning/access-management/what-is-mutual-tls/).

If you're on Kubernetes, [we wrote a TLS guide exclusive for it](https://vectorized.io/docs/tls-kubernetes/).

To enable TLS, set the `require_client_auth` field to `true` in the required listener's configuration. For example, to enable mTLS for the "external" API listener:

```yaml
redpanda:

  kafka_api:
  # The listener declaration. `name` can have any value.
  - name: internal
    address: <private IP>
    port: 9092

  advertised_kafka_api:
  # The advertised listeners. `name` should match the name of a declared listener.
  # The address:port here is what clients will connect to.
  - name: internal
    address: localhost
    port: 9092

  kafka_api_tls:
  # The listener's TLS config. `name` must match the corresponding listener's name.
  - name: internal
    enabled: true
    require_client_auth: true # <- This needs to be enabled!
    cert_file: <path to PEM-formatted cert file>
    key_file: <path to PEM-formatted key file>
    truststore_file: <path to PEM-formatted CA file>
```

If you're using rpk to interact with the Kafka API (managing topics or messages), pass the `--tls-key`, `--tls-cert`, and `--tls-truststore` flags to authenticate.

If you're interacting with the Admin API (managing users for example), pass the `--admin-api-tls-key`, `--admin-api-tls-cert`, and `--admin-api-tls-truststore` flags.

```bash
 rpk topic create test-topic \
--tls-key <path to PEM-formatted key file> \
--tls-cert <path to PEM-formatted cert file> \
--tls-truststore <path to PEM-formatted CA file>
```

The result will be: 

```bash
TOPIC       STATUS
test-topic  OK
```

And now, check the configuration of the topic with:

```bash
rpk topic describe test-topic <tls flags from above>
```

`rpk` defaults to connecting to `localhost:9092` for the Kafka protocol commands. If you are connecting to a remote broker or have configured your local broker differently, use the `--brokers <address:port>` flag. A further walkthrough of connecting with TLS and mTLS can be seen on [this page](https://vectorized.io/blog/tls-config/).

## ACLs

Access Control Lists (ACLs) allow you to provision users and provide each user fine grained access to specific resources. Redpanda provides ACLs through [SASL](https://en.wikipedia.org/wiki/Simple_Authentication_and_Security_Layer)/[SCRAM](https://en.wikipedia.org/wiki/Salted_Challenge_Response_Authentication_Mechanism), a secure and well defined authentication flow.

### Enabling SASL/SCRAM

To enable SASL/SCRAM, set `redpanda.enable_sasl` to `true` in the Redpanda configuration file and specify at least one "superuser" that will be used to bootstrap permissions for other users in the cluster.

Your config should look something like this:

```yaml
redpanda:
  enable_sasl: true
  superusers:
  - admin
  # The rest of the config...
```

<tabs>
  <tab id="Local Redpanda ">
You can modify the configuration file at `/etc/redpanda/redpanda.yaml`. After you modify the file, restart Redpanda for your changes to take effect.
  </tab>

  <tab id="Docker Container">  To change these configurations you can add this part in your docker-compose file:

```bash 
#start of config

    command:
    - redpanda
    - start
    - --set redpanda.enable_sasl=true
    - --set redpanda.superusers=["admin"]

#rest of config
```

  To check if they were applied successfully, open a bash for the container:

```bash
  docker exec -it <name-of-container> bash
```

Change `<name-of-container>` for your container and execute the command. 
Then run `cat` to check the config content:

```bash
  cat /etc/redpanda/redpanda.yaml
``` 

If everything is correct, you should see something like this:

```bash
config_file: /etc/redpanda/redpanda.yaml
node_uuid: ma7ms2Ut3a8E65RHJxh9kg3AcC4ocyLB3JfLCdkHtwY7PC6T3
pandaproxy: {}
redpanda:
  admin:
  - address: 0.0.0.0
    port: 9644
  advertised_kafka_api:
  - address: redpanda
    name: PLAINTEXT
    port: 29092
  - address: localhost
    name: OUTSIDE
    port: 9092
  auto_create_topics_enabled: true
  data_directory: /var/lib/redpanda/data
  developer_mode: true
  enable_sasl: true
  kafka_api:
  - address: 0.0.0.0
    name: PLAINTEXT
    port: 29092
  - address: 0.0.0.0
    name: OUTSIDE
    port: 9092
  node_id: 0
  rpc_server:
    address: 0.0.0.0
    port: 33145
  seed_servers: []
  superusers:
  - admin
rpk:
  coredump_dir: /var/lib/redpanda/coredump
  enable_memory_locking: false
  enable_usage_stats: true
  overprovisioned: true
  tune_aio_events: false
  tune_ballast_file: false
  tune_clocksource: false
  tune_coredump: false
  tune_cpu: false
  tune_disk_irq: false
  tune_disk_nomerges: false
  tune_disk_scheduler: false
  tune_disk_write_cache: false
  tune_fstrim: false
  tune_network: false
  tune_swappiness: false
  tune_transparent_hugepages: false
schema_registry: {}
```

  </tab>

  <tab id="Kubernetes ">
Change your configuration file in the YAML for the cluster. If you are using our [external-connectivity sample](https://raw.githubusercontent.com/vectorizedio/redpanda/dev/src/go/k8s/config/samples/external_connectivity.yaml), specify the `redpanda.enable_sasl` and `superuser` values in the cluster spec YAML file. For example: 

```yaml
apiVersion: redpanda.vectorized.io/v1alpha1
kind: Cluster
metadata:
  name: external-connectivity
spec:
  image: "vectorized/redpanda"
  version: "latest"
  replicas: 3
  enableSasl: true
  superusers:
  - admin
  resources:
    requests:
      cpu: 1
      memory: 2Gi
    limits:
      cpu: 1
      memory: 2Gi
  configuration:
    rpcServer:
      port: 33145
    kafkaApi:
     - port: 9092
     - external:
         enabled: true
    pandaproxyApi:
     - port: 8082
     - external:
         enabled: true
    adminApi:
    - port: 9644
    developerMode: true
```

> **_Note_** The attributes in k8s YAML are `camelCase` instead of `snake_case` as opposed to what's used in the redpanda.yaml configuration file.

After you change the configuration file, you must restart the pods for changes to take effect. You can check if your spec is correct by running: 

```
kubectl get clusters external-connectivity -o=jsonpath='{.spec}' 
```

  </tab>
</tabs>

#### Users

The `superusers` section in your `redpanda.yaml` is the user that you will use to bootstrap other users and ACLs. 

For example, if you used super user `admin`, Redpanda _allows_ the `admin` user to do anything, but Redpanda does _not_ create the `admin` user. You must now create this super user. 

Creating users happens through the Redpanda admin API, which defaults to `localhost:9644`. In the `rpk` commands below, if you have configured the admin API to use a different address/port, use the `--api-urls <address:port>` flag.

The reason you need a super user is because creating ACLs for users uses the Kafka protocol (default of `localhost:9092`, mentioned above). 

Since no ACLs exist at the start, you need a super user to bypass the requirement of needing ACLs to create ACLs. Once you provision an initial user and initial set of ACLs (described below), it is recommended to delete the super user.

You can create users and set passwords by running the following command:

<tabs>
  <tab id="Local Redpanda" >

```bash
rpk acl user create admin \
--p <password> \
--api-urls localhost:9644
```

  </tab>
  <tab id="Kubernetes">

```bash
kubectl exec -c redpanda external-connectivity-0 -- rpk acl user create admin \
--p <password> \
--api-urls localhost:9644 \
--brokers external-connectivity-0.external-connectivity.default.svc.cluster.local:9644
```

  </tab>
</tabs>

#### Connect to Redpanda

You can use the newly created user to interact with Redpanda with the Kafka protocol:

<tabs>
  <tab id="Local Redpanda" >

```bash
rpk topic describe test-topic \
--user admin \
--password <password> \
--brokers localhost:9092
```

  </tab>
  <tab id="Kubernetes">

```bash
kubectl exec -c redpanda external-connectivity-0 -- rpk topic describe test-topic \
--user admin \
--password <password> \
--brokers external-connectivity-0.external-connectivity.default.svc.cluster.local:9092
```

  </tab>
</tabs>

```bash
SUMMARY
=======
NAME        test-topic
PARTITIONS  1
REPLICAS    1

CONFIGS
=======
KEY                     VALUE       SOURCE
cleanup.policy          delete      DYNAMIC_TOPIC_CONFIG
compression.type        producer    DEFAULT_CONFIG
message.timestamp.type  CreateTime  DEFAULT_CONFIG
...
```

## Authorization

While **authentication** tells who you are, **authorization** tells you what can you do.

### Access Control Lists (ACLs)

[Access Control Lists](https://en.wikipedia.org/wiki/Access-control_list) (ACLs) is the main mechanism supported by Redpanda to manage user permissions.

Redpanda stores ACLs internally, replicated with [Raft](https://raft.github.io/) to provide the same consensus guarantees as your data. You can manage your ACLs with `rpk acl`.
 
Once you activate SASL, by default, only the super users will have access to the resources. It's recommended to create other users to effectively use Redpanda and then, create ACLs for them.

#### ACL Terminology

Entities accessing the **resources** are called **principals**. A User:foo is the principal for user "foo".

You can decide whether to to `allow` or `deny` **permissions** to access to the resources.
 
You can also specify from which **hosts** they will be allowed or denied access.
 
This access is represented as **operations**, such as `read`, `write`, or `describe`, and said operations can be performed on **resources**, such as a topic. You can filter the resources by name. 

#### Diving deeper

ACL commands work on a multiplicative basis. 

When you're creating if you set up two principals and two permissions the result will be four ACLs: both permissions for the first principal, as well as both permissions for the second principal. 

Adding two resources further doubles the ACLs created.

It is recommended to be as specific as possible when granting ACLs. 

Granting more ACLs than necessary per principal may inadvertently allow clients to do things they should not, such as deleting topics or joining the wrong consumer group.

#### Principals

All ACLs require a principal. 

A principal is composed of two parts: the type and the name. 

Within Redpanda, currently only one type is supported, "User". In the future Redpanda might add support for authorizing by Group or anything else.

When you create user "bar", Redpanda expects you to add ACLs for "User:bar". 

The `--allow-principal` and `--deny-principal` flags add this prefix for you if necessary. 

The special name '\*' matches any name, meaning an ACL with principal "User:\*" grants or denies the permission for any user.

#### Hosts

Hosts can be seen as an extension of the principal, and effectively gate where the principal can connect from. 

When creating ACLs, unless otherwise specified, the default host is the wildcard `*` which allows or denies the principal from all hosts.

If specifying hosts, you must pair the `--allow-host` flag with the `--allow-principal` flag

The same applies to the `--deny-host` flag with the `--deny-principal` flag.

#### Resources

A resource is what an ACL allows or denies access to. 

There are four resources within Redpanda: `topics`, `groups`, `cluster`, and `transactionalid`.

Names for each of these resources can be specified with their respective flags.

Resources combine with the operation that is allowed or denied on that resource. 

By default, resources are specified on an exact name match (a "literal" match).

The `--resource-pattern-type` flag can be used to specify that a resource name is "prefixed", meaning to allow anything with the given prefix. 

A literal name of "foo" will match only the topic "foo", while the prefixed name of "foo-" will match both "foo-bar" and "foo-jazz". 

The special wildcard resource name '\*' matches any name of the given resource type (`--topic` '\*' matches all topics).

#### Operations

Pairing with resources, operations are the actions that are allowed or denied.
Redpanda has the following operations:

<table>
<tbody>
<tr>
<td>Operation&nbsp;</td>
<td>Description</td>
</tr>
<tr>
<td>&nbsp;ALL</td>
<td>&nbsp;Allows all operations below.</td>
</tr>
<tr>
<td>READ</td>
<td>&nbsp;Allows reading a given resource.</td>
</tr>
<tr>
<td>
<div>
<div>WRITE</div>
</div>
</td>
<td>&nbsp;Allows writing to a given resource.</td>
</tr>
<tr>
<td>
<div>
<div>CREATE</div>
</div>
</td>
<td>&nbsp;Allows creating a given resource.</td>
</tr>
<tr>
<td>
<div>
<div>DELETE</div>
</div>
</td>
<td>&nbsp;Allows deleting a given resource</td>
</tr>
<tr>
<td>
<div>
<div>ALTER</div>
</div>
</td>
<td>&nbsp;Allows altering non-configurations.</td>
</tr>
<tr>
<td>DESCRIBE</td>
<td>&nbsp;Allows querying non-configurations.</td>
</tr>
<tr>
<td>DESCRIBE_CONFIGS</td>
<td>&nbsp;Allows describing configurations.</td>
</tr>
<tr>
<td>
<div>
<div>ALTER_CONFIGS</div>
</div>
</td>
<td>&nbsp;Allows altering configurations.</td>
</tr>
</tbody>
</table>

#### Producing/Consuming

The following lists the operations needed for each individual client request, where **resource** corresponds to the resource flag, and "for xyz" corresponds to the resource name(s) in the request:

```bash
The following lists the operations needed for each individual client request,
where "on RESOURCE" corresponds to the resource flag, and "for xyz" corresponds
to the resource name(s) in the request:

PRODUCING/CONSUMING

    Produce      WRITE on TOPIC for topics
                 WRITE on TRANSACTIONAL_ID for the transaction.id

    Fetch        READ on TOPIC for topics

    ListOffsets  DESCRIBE on TOPIC for topics

    Metadata     DESCRIBE on TOPIC for topics
                 CREATE on CLUSTER for kafka-cluster (if automatically creating topics)
                 CREATE on TOPIC for topics (if automatically creating topics)

    OffsetForLeaderEpoch  DESCRIBE on TOPIC for topics

GROUP CONSUMING

    FindCoordinator  DESCRIBE on GROUP for group
                     DESCRIBE on TRANSACTIONAL_ID for transactional.id (transactions)

    OffsetCommit     READ on GROUP for groups
                     READ on TOPIC for topics

    OffsetFetch      DESCRIBE on GROUP for groups
                     DESCRIBE on TOPIC for topics

    OffsetDelete     DELETE on GROUP for groups
                     READ on TOPIC for topics

    JoinGroup        READ on GROUP for group
    Heartbeat        READ on GROUP for group
    LeaveGroup       READ on GROUP for group
    SyncGroup        READ on GROUP for group

TRANSACTIONS (including FindCoordinator above)

    AddPartitionsToTxn  WRITE on TRANSACTIONAL_ID for transactional.id
                        WRITE on TOPIC for topics

    AddOffsetsToTxn     WRITE on TRANSACTIONAL_ID for transactional.id
                        READ on GROUP for group

    EndTxn              WRITE on TRANSACTIONAL_ID for transactional.id

    TxnOffsetCommit     WRITE on TRANSACTIONAL_ID for transactional.id
                        READ on GROUP for group
                        READ on TOPIC for topics

ADMIN

    CreateTopics      CREATE on CLUSTER for kafka-cluster
                      CREATE on TOPIC for topics
                      DESCRIBE_CONFIGS on TOPIC for topics, for returning topic configs on create

    CreatePartitions  ALTER on TOPIC for topics

    DeleteTopics      DELETE on TOPIC for topics
                      DESCRIBE on TOPIC for topics, if deleting by topic id (in addition to prior ACL)

    DeleteRecords     DELETE on TOPIC for topics

    DescribeGroup     DESCRIBE on GROUP for groups

    ListGroups        DESCRIBE on GROUP for groups
                      or, DESCRIBE on CLUSTER for kafka-cluster

    DeleteGroups      DELETE on GROUP for groups

    DescribeConfigs   DESCRIBE_CONFIGS on CLUSTER for cluster (broker describing)
                      DESCRIBE_CONFIGS on TOPIC for topics (topic describing)

    AlterConfigs      ALTER_CONFIGS on CLUSTER for cluster (broker altering)
                      ALTER_CONFIGS on TOPIC for topics (topic altering)

```

You can also get this information at the CLI by running: 

```bash
rpk acl --help-operations 
```

In flag form to set up a general producing/consuming client, you can invoke `rpk acl create` up to three times with the following (including your `--allow-principal`):

```bash
--operation write,read,describe --topic [topics]
--operation describe,read --group [group.id]
--operation describe,write --transactional-id [transactional.id]
```

#### Permissions 

A client can be allowed access or denied access. By default, all permissions are denied. 

You only need to specifically deny a permission if you allow a wide set of permissions and then want to deny a specific permission in that set.

You could allow all operations, and then specifically deny writing to topics.

#### Management

Creating ACLs works on a specific ACL basis, but listing and deleting ACLs works on filters. 

Filters allow matching many ACLs to be printed listed and deleted at once. 

Because this can be risky for deleting, the delete command prompts for confirmation by default. 

### RPK ACL & Management of users 

`rpk acl` is a command made to both manage your ACLs as well as your SASL users.

If you're on Kubernetes you can use `kubectl exec` to run RPK's commands.

Here's the general usage:
 
```bash
rpk acl [command] [flags]
```
 
For example, to create an user:
 
```bash
rpk acl user create \
--new-username Jack \
--new-password <password> \
--api-urls localhost:9644
```
 
Results in:
 
```bash
Created user 'Jack'
```
 
Here are all the available commands and how they interact with Redpanda:
 
<table>
<tbody>
<tr>
<td>Command</td>
<td>Protocol</td>
<td>Default Port</td>
</tr>
<tr>
<td>user</td>
<td>Admin API</td>
<td>9644</td>
</tr>
<tr>
<td>list</td>
<td>Kafka API</td>
<td>9092</td>
</tr>
<tr>
<td>create</td>
<td>Kafka API</td>
<td>9092</td>
</tr>
<tr>
<td>delete</td>
<td>Kafka API</td>
<td>9092</td>
</tr>
</tbody>
</table>

 
You can always run `rpk acl -h` to get more information.
 
#### Global Flags
 
Every `rpk acl` command can have these flags:
 
<table>
<tbody>
<tr>
<td>Flag</td>
<td>Description</td>
</tr>
<tr>
<td>--admin-api-tls-cert</td>
<td>The certificate to be used for TLS authentication with the Admin API.</td>
</tr>
<tr>
<td>--admin-api-tls-enabled</td>
<td>Enable TLS for the Admin API (not necessary if specifying custom certs). This is assumed as true when passing other --admin-api-tls flags.</td>
</tr>
<tr>
<td>--admin-api-tls-key</td>
<td>The certificate key to be used for TLS authentication with the Admin API.</td>
</tr>
<tr>
<td>--admin-api-tls-truststore</td>
<td>The truststore to be used for TLS communication with the Admin API.</td>
</tr>
<tr>
<td>--brokers</td>
<td>Comma-separated list of broker ip:port pairs (e.g. --brokers '192.168.78.34:9092,192.168.78.35:9092,192.179.23.54:9092' ). Alternatively, you may set the REDPANDA_BROKERS environment variable with the comma-separated list of broker addresses.</td>
</tr>
<tr>
<td>--config</td>
<td>Redpanda config file, if not set the file will be searched for in the default locations</td>
</tr>
<tr>
<td>-h, --help</td>
<td>Help for acl.</td>
</tr>
<tr>
<td>--password</td>
<td>SASL password to be used for authentication.</td>
</tr>
<tr>
<td>--sasl-mechanism</td>
<td>The authentication mechanism to use. Supported values: SCRAM-SHA-256, SCRAM-SHA-512.</td>
</tr>
<tr>
<td>--tls-cert</td>
<td>The certificate to be used for TLS authentication with the broker.</td>
</tr>
<tr>
<td>--tls-enabled</td>
<td>Enable TLS for the Kafka API (not necessary if specifying custom certs).This is assumed as true when passing other --tls flags.</td>
</tr>
<tr>
<td>--tls-key</td>
<td>The certificate key to be used for TLS authentication with the broker.</td>
</tr>
<tr>
<td>--tls-truststore</td>
<td>The truststore to be used for TLS communication with the broker.</td>
</tr>
<tr>
<td>--user</td>
<td>SASL user to be used for authentication.</td>
</tr>
</tbody>
</table>


#### Create ACLs
 
Following the multiplying effect of combining flags, the create command works on a straightforward basis: every ACL combination is a created ACL.

At least one principal, one host, one resource, and one operation is required to create a single ACL.

Here's the general usage:
 
```bash
rpk acl create/delete [globalACLFlags] [localFlags]
```
 
You can use the global flags that we saw before and some other local flags. Here's the available local flags:
 
<table>
<tbody>
<tr>
<td>Flag</td>
<td>Description</td>
</tr>
<tr>
<td>--allow-host</td>
<td>Host for which access will be granted. (repeatable)</td>
</tr>
<tr>
<td>--allow-principal</td>
<td>Principals to which permissions will be granted. (repeatable)</td>
</tr>
<tr>
<td>--cluster</td>
<td>Whether to grant ACLs to the cluster.</td>
</tr>
<tr>
<td>--deny-host</td>
<td>Host from which access will be denied. (repeatable)</td>
</tr>
<tr>
<td>--deny-principal</td>
<td>Principal to which permissions will be denied. (repeatable)</td>
</tr>
<tr>
<td>--group</td>
<td>Group to grant ACLs for. (repeatable)</td>
</tr>
<tr>
<td>-h, --help</td>
<td>Help for create.</td>
</tr>
<tr>
<td>--name-pattern</td>
<td>The name pattern type to be used when matching the resource names.</td>
</tr>
<tr>
<td>--operation</td>
<td>Operation that the principal will be allowed or denied. Can be passed many times.</td>
</tr>
<tr>
<td>--resource-pattern-type</td>
<td>Pattern to use when matching resource names (literal or prefixed) (default "literal")</td>
</tr>
<tr>
<td>--topic</td>
<td>Topic to grant ACLs for. (repeatable)</td>
</tr>
<tr>
<td>--transactional-id</td>
<td>Transactional IDs to grant ACLs for. (repeatable)</td>
</tr>
</tbody>
</table>

Examples: 

Allow all permissions to user bar on topic "foo" and group "g":

```bash
rpk acl create --allow-principal bar --operation all --topic foo --group g
```

Allow read permissions to all users on topics biz and baz:

```bash
rpk acl create --allow-principal '*' --operation read --topic biz,baz
```

Allow write permissions to user buzz to transactional id "txn":

```bash
rpk acl create --allow-principal User:buzz --operation write --transactional-id txn
```

#### List/Delete ACLs

List and Delete work in a similar multiplying effect as creating ACLs, but delete is more advanced.

They work on a filter basis. Any unspecified flag defaults to matching everything (all operations, or all allowed principals, etc). 

To ensure that you do not accidentally delete more than you intend, this command prints everything that matches your input filters and prompts for a confirmation before the delete request is issued. 

Anything matching more than 10 ACLs is going to ask again for confirmation.

If no resources are specified, all resources are matched. If no operations are specified, all operations are matched. 

You can also opt in to matching everything. `--operation any` matches any operation, for example.

The `--resource-pattern-type`, defaulting to `any`, configures how to filter resource names:
  * `any` returns exact name matches of either prefixed or literal pattern type
  * `match` returns wildcard matches, prefix patterns that match your input, and literal matches
  * `prefix` returns prefix patterns that match your input (prefix "fo" matches "foo")
  * `literal` returns exact name matches

Here's the general usage:
 
```bash
rpk acl list/delete [globalACLFlags] [localFlags]
```
 
You can use the global flags that we saw before and some other local flags. Here's the available local flags:

<table>
<tbody>
<tr>
<td>&nbsp;Flag</td>
<td>Description&nbsp;</td>
</tr>
<tr>
<td>--allow-host&nbsp;</td>
<td>Allowed host ACLs to list/remove. (repeatable)</td>
</tr>
<tr>
<td>&nbsp;--allow-principal</td>
<td>Allowed principal ACLs to list/remove. (repeatable)</td>
</tr>
<tr>
<td>&nbsp;--cluster</td>
<td>Whether to list/remove ACLs to the cluster.</td>
</tr>
<tr>
<td>&nbsp;--deny-host</td>
<td>Denied host ACLs to list/remove. (repeatable)</td>
</tr>
<tr>
<td>&nbsp;--deny-principal</td>
<td>Denied principal ACLs to list/remove. (repeatable)</td>
</tr>
<tr>
<td>&nbsp;-d, --dry&nbsp;</td>
<td>Dry run: validate what would be deleted.</td>
</tr>
<tr>
<td>&nbsp;--group</td>
<td>Group to list/remove ACLs for. (repeatable)</td>
</tr>
<tr>
<td>&nbsp;-h, --help</td>
<td>Help for delete.</td>
</tr>
<tr>
<td>&nbsp;--no-confirm&nbsp;</td>
<td>Disable confirmation prompt.</td>
</tr>
<tr>
<td>&nbsp;--operation</td>
<td>Operation to list/remove. (repeatable)</td>
</tr>
<tr>
<td>&nbsp;-f, --print-filters</td>
<td>Print the filters that were requested. (failed filters are always printed)</td>
</tr>
<tr>
<td>--resource-pattern-type</td>
<td>Pattern to use when matching resource names. (any, match, literal, or prefixed) (default "any")</td>
</tr>
<tr>
<td>--topic</td>
<td>Topic to list/remove ACLs for. (repeatable)</td>
</tr>
<tr>
<td>--transactional-id&nbsp;</td>
<td>Transactional IDs to list/remove ACLs for. (repeatable)</td>
</tr>
</tbody>
</table>



#### User
 
Used to manage the SASL users.

If SASL is enabled, a SASL user is what you use to talk to Redpanda, and ACLs control what your user has access to. 

Using SASL requires setting "enable_sasl: true" in the redpanda section of your redpanda.yaml.

Here's the general usage:
 
```bash
rpk acl user [command] [globalACLFlags] [globalUserFlags]
```
 
For users you can use the global flags that we saw before and these global user flags. Here's the available global user flags:
 
<table>
<tbody>
<tr>
<td>Flag</td>
<td>Description</td>
<td>Supported Value</td>
</tr>
<tr>
<td>--api-urls</td>
<td>The comma-separated list of Admin API addresses (IP:port). You must specify one for each node.</td>
<td><strong><em>strings</em></strong></td>
</tr>
<tr>
<td>-h, --help</td>
<td>Help for user.</td>
<td></td>
</tr>
</tbody>
</table>


##### User create

This command creates a single SASL user with the given password, optionally with a custom "mechanism". 

SASL consists of three parts: a username, a password, and a mechanism. 

The mechanism determines which authentication flow the client will use for this user/pass.

Redpanda currently supports two mechanisms: SCRAM-SHA-256, the default, and SCRAM-SHA-512, which is the same flow but uses sha512 rather than sha256.

Before a created SASL account can be used, you must also create ACLs to grant the account access to certain resources in your cluster.

Here's the general usage:
 
```bash
rpk acl user create [USER] -p [PASSWORD] [globalACLFlags] [globalUserFlags] [localFlags]
```
 
Here's the local flags:
<table>
<tbody>
<tr>
<td>Flag</td>
<td>Description</td>
</tr>
<tr>
<td>-h, --help</td>
<td>Help for create.</td>
</tr>
<tr>
<td>--mechanism</td>
<td>SASL mechanism to use (scram-sha-256, scram-sha-512, case insensitive) (default "scram-sha-256")</td>
</tr>
</tbody>
</table>


#### User delete
 
This command deletes the specified SASL account from Redpanda. This does not delete any ACLs that may exist for this user. 

You may want to recreate the user later, as well, not all ACLs have users that they describe (instead they are for wildcard users).

Here's the general usage:
 
```bash
rpk acl user delete [USER] [globalACLFlags] [globalUserFlags] 
```

#### User list
 
Used to list SASL users. Here's the general usage:
 
```bash
rpk acl user list [globalACLFlags] [globalUserFlags]
```
 
You can also use the shortened version changing `list` to `ls`.