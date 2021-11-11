---
title: Authorization & authentication
order: 1
---

# Authorization & authentication

Security should be at the heart of the design of any software project, and Redpanda is not an exception. Today, I wanna introduce the different built-in mechanisms by which you will be able to make your Redpanda cluster more secure. By following this guide, you will familiarize yourself with the available authorization and authentication methods that Redpanda support, both at a conceptual level and at a technical level through hands-on examples.

**A note on scope:** Let's emphasize the word _built-in_ in the previous paragraph. Security is a goal achieved through many different aspects, such as network configuration, organization-wide roles management, among many others. However, they are external to Redpanda so this guide will not cover them.

All the concepts described here are compatible with the current version of Kafka® and its client libraries and CLIs.  

## Prerequisites

- A running redpanda node.

If you haven't checked them out, you can follow our [Getting Started guides](https://vectorized.io/docs/), where you'll find how to get started with Redpanda.

- TLS certificates.

If you have your own certificates, either self-signed or issued by a trusted Certificate Authority, you can use them for this guide. Otherwise, feel free to [use our prepared script](https://gist.github.com/0x5d/56422a0c447e58d8ccbfa0ce1fd6bac6) to generate certificates or simply run the command:

```bash
bash <(curl -s https://gist.githubusercontent.com/0x5d/56422a0c447e58d8ccbfa0ce1fd6bac6/raw/933ca97702f6b844f706b674133105a30bdad3ff/generate-certs.sh)
```

## Authentication

### Mutual TLS (mTLS)

Mutual TLS, or 2-way TLS, is an authentication method in which the server keeps a set of trusted certificates in the form of a "truststore" file, and all clients attempting to establish a connection must present their certificate.

If you're on Kubernetes, make sure you set up the [connectivity in the Kubernetes network](https://vectorized.io/docs/kubernetes-connectivity/).

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
    truststore_file: <path to PEM-formatted truststore file>
```

On `rpk`'s side, if you're interacting with the Kafka API (managing topics or messages), then you may pass  `--tls-key`, `--tls-cert` and `--tls-truststore` flags. 

If you're interacting with the Admin API (managing users for example), then you may pass  `--admin-api-tls-key`, `--admin-api-tls-cert` and `--admin-api-tls-truststore` flags.

```bash
 rpk topic create test-topic \
--tls-key <path to PEM-formatted key file> \
--tls-cert <path to PEM-formatted cert file> \
--tls-truststore <path to PEM-formatted truststore file>
```
The result will be: 

```bash
Created topic 'test-topic'.
```

Check the configuration of the topic with:

```bash
rpk topic describe 'test-topic'
```

> **_Note_** - `rpk` defaults to connecting to `localhost:9092` for the Kafka API commands. If you're connecting to a remote broker, pass `--brokers <node IP>:<kafka API port>`

> You can also connect with [TLS and mutual TLS encryption](https://vectorized.io/blog/tls-config/).

### SASL/ SCRAM

Redpanda also supports client authentication with usernames and passwords using SASL/SCRAM ([Simple Authentication and Security Layer protocol](https://en.wikipedia.org/wiki/Simple_Authentication_and_Security_Layer) with [Salted Challenge Response Authentication Mechanism)](https://en.wikipedia.org/wiki/Salted_Challenge_Response_Authentication_Mechanism).

#### Enabling SASL/ SCRAM

To enable SASL/SCRAM, set `redpanda.enable_sasl` to `true` in the configuration file and specify at least one "superuser" to give permissions to for all operations on the clusters.

Your config should look something like this:

```yaml
redpanda
  
  enable_sasl: true
  superusers:
    - username: admin
  # The rest of the config...
```

<tabs>
  <tab id="Local Redpanda ">
  You can find the configuration file here

  ```bash
  /etc/redpanda/redpanda.yaml
  ```

After you change the config, restart Redpanda service for changes to take effect. 

  </tab>
  <tab id="Docker Container">
  To access the files inside the container, first you have to enter the container.

  You can do that by running:

```bash
  docker exec -it <name-of-container> bash
```

Change `<name-of-container>` for your container and execute the command. 
Then go to the same directory as a local redpanda config:

```bash
  /etc/redpanda/redpanda.yaml
```

After you finish it, restart the container for changes to take effect. 

  </tab>
  <tab id="Kubernetes ">
  For Kubernetes, things are a little different. You can change your configuration file with the YAML for the cluster.

  If you're using our [external-connectivity sample](https://raw.githubusercontent.com/vectorizedio/redpanda/dev/src/go/k8s/config/samples/external_connectivity.yaml), specify the `redpanda.enable_sasl` and `superuser` values in the cluster spec YAML file.

  For example: 

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
  superUsers:
    - username: admin
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

> **_Note_** - The attributes in K8s YAML are in camelCase instead of snake_case used in local redpanda.yaml

Remember, that after you change the config file you **must** restart the pods for changes to take effect.

You can check if your spec is correct by running: 

```
kubectl get clusters external-connectivity -o=jsonpath='{.spec}' 
```

  </tab>
</tabs>

#### Creating admin user

Create a new user and set a password by running the following command. Replace `<password>` for a password of your choice. 

<tabs>
  <tab id="Local Redpanda" >

```bash
rpk acl user create \
--new-username admin \
--new-password <password> \
--api-urls localhost:9644
```

  </tab>
  <tab id="Kubernetes">

```bash
kubectl exec -c redpanda external-connectivity-0 -- rpk acl user create \
--new-username admin \
--new-password <password> \
--api-urls localhost:9644 \ 
--brokers external-connectivity-0.external-connectivity.default.svc.cluster.local:9644
```

  </tab>
</tabs>


The response should be:

```
Created user 'admin'
```

> **_Note_** - `rpk acl` is an Admin API command so it defaults to connecting to `localhost:9644`. If you have a different IP address, URL, or port, pass `--api-urls <node IP>:<admin API port>`.

#### Connect to Redpanda

You're now able to use the created identity to interact with the Kafka API. For example:

<tabs>
  <tab id="Local Redpanda" >

```bash
rpk topic describe test-topic \
--user admin \
--password <password> \
--sasl-mechanism SCRAM-SHA-256 \
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

The response should look something like this: 

```bash
  Name                test-topic  
  Internal            false       
  Cleanup policy      delete      
  Config:             
  Name                Value       Read-only  Sensitive  
  partition_count     1           false      false      
  replication_factor  1           false      false      
  cleanup.policy      delete      false      false      
  Partitions          1 - 1 out of 1  
  Partition           Leader          Replicas   In-Sync Replicas  High Watermark  
  0                   1               [1]        [1]               0               
```

> **_Note_** - If you still have the TLS config from the previous section, you'll also need to pass the TLS flags.

Notice that this command uses the Kafka API, so we're using the `--brokers <node IP>:<kafka API port>` pattern here, having the default value as `localhost:9092`.

## Authorization

While **authentication** is about making sure that whatever client connects to your cluster is trusted — or, to put it in other words, who they say they are —, **authorization** is about making sure that each client has access to exactly the data it should.

### Access Control Lists (ACLs)

[Access Control Lists](https://en.wikipedia.org/wiki/Access-control_list) (ACLs) is the main mechanism supported by Redpanda to manage user permissions.
 
Usually, other Kafka implementations use Apache ZooKeeper™ to store and manage ACLs. However, since Redpanda doesn't use Zookeeper, we have effectively built our own ACL implementation to store and manage it. You can manage your ACLs with `rpk`.
 
ACLs are stored inside Redpanda's control nodes so we don't need an external store for config. 

Once you activate SASL, by default, only the super users will have access to the resources. It's recommended to create other users to effectively use Redpanda.

### ACL Terminology

Entities accessing the **resources** are called **principals**. User and Group (as in UserGroup), for example, are 2 different types of principals.
 
Principals are allowed or denied access to resources, and you can also specify from which **hosts** they will be allowed or denied access.
 
This access is represented as **operations**, such as `read`, `write`, or `describe`, and said operations can be performed on **resources**, such as a topic. You can filter the resources by name.
 
### RPK ACL
 
You can interact with ACLs in the CLI using `rpk acl` commands. If you're on Kubernetes you can use `kubectl exec` to run RPK's commands. 

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


#### Create/Delete ACLs
 
Used to create or delete ACLs. Here's the general usage:
 
```bash
rpk acl create/delete [globalFlags] [localFlags]
```
 
You can use the global flags that we saw before and some other local flags. Here's the available local flags:
 
<table>
<tbody>
<tr>
<td>Flag</td>
<td>Description</td>
<td>Supported values</td>
</tr>
<tr>
<td>--allow-host</td>
<td>Host for which access will be granted. Can be passed as a comma-separated list.</td>
<td><strong><em>strings</em></strong></td>
</tr>
<tr>
<td>--allow-principal</td>
<td>Principal to which permissions will be granted. Can be passed as a comma-separated list.</td>
<td><strong><em>strings</em></strong></td>
</tr>
<tr>
<td>--deny-host</td>
<td>Host from which access will be denied. Can be passed as a comma-separated list.</td>
<td><strong><em>strings</em></strong></td>
</tr>
<tr>
<td>--deny-principal</td>
<td>Principal to which permissions will be denied. Can be passed as a comma-separated list.</td>
<td><strong><em>strings</em></strong></td>
</tr>
<tr>
<td> -h, --help</td>
<td>Help for create.</td>
<td></td>
</tr>
<tr>
<td>--name-pattern</td>
<td>The name pattern type to be used when matching the resource names.</td>
<td>any, match, literal, prefixed. (default "literal")</td>
</tr>
<tr>
<td>--operation</td>
<td>Operation that the principal will be allowed or denied. Can be passed many times.</td>
<td>any, all, read, write, create, delete, alter, describe, clusteraction, describeconfigs, alterconfigs, idempotentwrite</td>
</tr>
<tr>
<td>--resource</td>
<td>The target resource for the ACL.</td>
<td>any, cluster, group, topic, transactionalid</td>
</tr>
<tr>
<td>--resource-name</td>
<td>The name of the target resource for the ACL.</td>
<td><strong><em>string</em></strong></td>
</tr>
</tbody>
</table>


#### List ACLs
 
Used to list the ACLs. Here's the general usage:
 
```bash
rpk acl list [globalFlags] [localFlags]
```
 
You can also use the shortened version changing `list` to `ls`.
 
You can use the global flags that we saw before and some other local flags. Here's the available local flags:
 
<table>
<tbody>
<tr>
<td>Flag</td>
<td>Description</td>
<td>Supported Values</td>
</tr>
<tr>
<td>-h, --help</td>
<td>Help for list.</td>
<td></td>
</tr>
<tr>
<td>--host</td>
<td>Host to filter by. Can be passed multiple times as a comma-separated list to filter by many hosts.</td>
<td><strong><em>strings</em></strong></td>
</tr>
<tr>
<td>--name-pattern</td>
<td>The name pattern type to be used when matching affected resources.</td>
<td>any, match, literal, prefixed.</td>
</tr>
<tr>
<td>--operation</td>
<td>Operation to filter by. Can be passed as a comma-separated list multiple times to filter by many operations.</td>
<td>any, all, read, write, create, delete, alter, describe, clusteraction, describeconfigs, alterconfigs, idempotentwrite</td>
</tr>
<tr>
<td>--permission</td>
<td>Permission to filter by. Can be passed as a comma-separated list multiple times to filter by many permissions.</td>
<td>any, deny, allow.</td>
</tr>
<tr>
<td>--principal</td>
<td>Principal to filter by. Can be passed as a comma-separated list multiple times to filter by many principals.</td>
<td><strong><em>strings</em></strong></td>
</tr>
<tr>
<td>--resource</td>
<td>Resource type to filter by.</td>
<td>any, cluster, group, topic, transactionalid.</td>
</tr>
<tr>
<td>--resource-name</td>
<td>The name of the resource of the given type.</td>
<td><strong><em>string</em></strong></td>
</tr>
</tbody>
</table>


#### User
 
Used to manage the users. Here's the general usage:
 
```bash
rpk acl user [command] [globalFlags] [globalUserFlags]
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
 
Used to create a user. Here's the general usage:
 
```bash
rpk acl user create [globalFlags] [globalUserFlags] [localFlags]
```
 
Here's the local flags:
<table>
<tbody>
<tr>
<td>Flag</td>
<td>Description</td>
<td>Supported Value</td>
</tr>
<tr>
<td>-h, --help</td>
<td>Help for create.</td>
<td></td>
</tr>
<tr>
<td>--new-password</td>
<td>The new user's password.</td>
<td><strong><em>string</em></strong></td>
</tr>
<tr>
<td>--new-username</td>
<td>The user to be created.</td>
<td><strong><em>string</em></strong></td>
</tr>
</tbody>
</table>


#### User delete
 
Used to delete an user. Here's the general usage:
 
```bash
rpk acl user delete [globalFlags] [globalUserFlags] [localFlags]
```
 
Here's the available local flags:
 
<table>
<tbody>
<tr>
<td>Flag</td>
<td>Description</td>
<td>Supported Value</td>
</tr>
<tr>
<td>-h, --help</td>
<td>Help for delete.</td>
<td></td>
</tr>
<tr>
<td>--delete-username</td>
<td>The user to be deleted.</td>
<td><strong><em>string</em></strong></td>
</tr>
</tbody>
</table>


#### User list
 
Used to list the users. Here's the general usage:
 
```bash
rpk acl user list [globalFlags] [globalUserFlags] [localFlags]
```
 
You can also use the shortened version changing `list` to `ls`.
 
Here's the available local flags:
 
<table>
<tbody>
<tr>
<td>Flag</td>
<td>Description</td>
<td>Supported Value</td>
</tr>
<tr>
<td> -h, --help</td>
<td>Help for user list.</td>
<td></td>
</tr>
<tr>
<td>--host</td>
<td>Host to filter by. Can be passed multiple times as a comma-separated list to filter by many hosts.</td>
<td><strong><em>strings</em></strong></td>
</tr>
<tr>
<td>--name-pattern</td>
<td>The name pattern type to be used when matching affected resources.</td>
<td><strong><em>string</em></strong></td>
</tr>
<tr>
<td>--operation</td>
<td>Operation to filter by. Can be passed multiple times as a comma-separated list to filter by many operations.</td>
<td>any, all, read, write, create, delete, alter, describe, clusteraction, describeconfigs, alterconfigs, idempotentwrite</td>
</tr>
<tr>
<td>--permission</td>
<td>Permission to filter by. Can be passed multiple times as a comma-separated list to filter by multiple permission types.</td>
<td>any, deny, allow.</td>
</tr>
<tr>
<td>--principal</td>
<td>Principal to filter by. Can be passed multiple times as a comma-separated list to filter by many principals.</td>
<td><strong><em>strings</em></strong></td>
</tr>
<tr>
<td>--resource</td>
<td>Resource type to filter by.</td>
<td>any, cluster, group, topic, transactionalid.</td>
</tr>
<tr>
<td>--resource-name</td>
<td>The name of the resource of the given type.</td>
<td><strong><em>string</em></strong></td>
</tr>
</tbody>
</table>