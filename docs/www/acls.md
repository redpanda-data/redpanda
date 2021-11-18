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

If you're on Kubernetes, make sure you set up [connectivity in the Kubernetes network](https://vectorized.io/docs/kubernetes-connectivity/).

To enable it, set the `require_client_auth` field to `true` in the required listener's configuration. For example, to enable mTLS for the "external" API listener:

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

On `rpk`'s side, you can use the `--tls-key`, `--tls-cert` and `--tls-truststore` flags to have it authenticate and establish a TLS connection:

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

Redpanda also supports client authentication with usernames and passwords using SASL/SCRAM ([Simple Authentication and Security Layer protocol](https://en.wikipedia.org/wiki/Simple_Authentication_and_Security_Layer) using [Salted Challenge Response Authentication Mechanism)](https://en.wikipedia.org/wiki/Salted_Challenge_Response_Authentication_Mechanism).

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

Refer to the next section, _Managing users_, for more details on `rpk acl user`.

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

#### Managing users

While having a superuser is a must to get started, it's not really a good idea to either share the superuser's credentials everywhere or to make everyone a superuser.

You can create, delete and list users with `rpk acl user`.

`rpk acl` is an Admin API command so it defaults to connecting to `localhost:9644`. If you have a different IP address, URL, or port, pass `--api-urls <node IP>:<admin API port>`.

Replace `<password>` for a password of your choice. 

**Creating a user**

<tabs>

  <tab id="Local Redpanda">

```bash
rpk acl user create \
--new-username Jack \
--new-password <password> \
--api-urls localhost:9644
```

```bash 
Created user 'Jack'
```

**Deleting a user**

```bash
rpk acl user delete \
--delete-username Jack \
--api-urls localhost:9644
```

```bash
Deleted user 'Jack'
```

**Listing users**

```bash
rpk acl user list \
--api-urls localhost:9644
```

```bash
USERNAME                        
                                
Michael                         
Jim                             
Pam                             
Dwight                          
Kelly
```

  </tab>

  <tab id="Kubernetes">

```bash
kubectl exec -c redpanda external-connectivity-0 -- rpk acl user create \
--new-username Jack \
--new-password <password> \
--api-urls localhost:9644 \ 
--brokers external-connectivity-0.external-connectivity.default.svc.cluster.local:9644
```

```bash 
Created user 'Jack'
```

**Deleting a user**

```bash
kubectl exec -c redpanda external-connectivity-0 -- rpk acl user delete \
--delete-username Jack \
--api-urls localhost:9644 \ 
--brokers external-connectivity-0.external-connectivity.default.svc.cluster.local:9644
```

```bash
Deleted user 'Jack'
```

**Listing users**

```bash
kubectl exec -c redpanda external-connectivity-0 -- rpk acl user list \
--api-urls localhost:9644 \ 
--brokers external-connectivity-0.external-connectivity.default.svc.cluster.local:9644
```

```bash
USERNAME                        
                                
Michael                         
Jim                             
Pam                             
Dwight                          
Kelly
```

  </tab>

</tabs>



## Authorization

While **authentication** is about making sure that whatever client connects to your cluster is trusted — or, to put it in other words, who they say they are —, **authorization** is about making sure that each client has access to exactly the data it should.

ACLs is the main mechanism supported by Redpanda to manage user permissions.

### Access Control Lists (ACLs)

At a high level, ACLs specify what users can or cannot do. They can be managed with rpk, which supports creating, deleting or listing ACLs through `rpk acl create`, `rpk acl delete` and `rpk acl list`, respectively.

In ACL terminology, the entities accessing the **resources** are called "**principals**". "User" and "Group" (as in "consumer group") are 2 different types of principals.

Principals are allowed or denied access to resources, and you can also specify from which **hosts** they will be allowed or denied access.

This "access" is represented as "**operations**", such as "read", "write", or "describe", and said operations can be performed on **resources**, such as a topic. You can further filter the resources by name, 

Here's a reference of all the concepts and their supported values.

#### **Permissions**

`any`, `allow`, `deny`.

#### **Operations**

`any`, `all`, `read`, `write`, `create`, `delete`, `alter`, `describe`, `clusteraction`, `describeconfigs`, `alterconfigs`, `idempotentwrite`.

#### **Resources**

`*`, `cluster`, `group`, `topic`, `transactionalid`.

#### **rpk acl quick reference**

Please note that you may have to add the `--tls-key`, `--tls-cert` and `--tls-truststore` flags, as well as `--user`, `--password` & `--sasl-mechanism` if mTLS and SASL/ SCRAM are enabled (recommended). They are omitted here for brevity.

**Creating ACLs**

- Create an ACL allowing a user to perform all operations from all hosts to a topic named "pings":

```bash
 rpk acl create \
  --allow-principal 'User:Charlie' \
  --allow-host '*' \
  --operation all \
  --resource topic \
  --resource-name pings

Created ACL for principal 'User:Charlie' with host '*', operation 'All' and permission 'Allow'
```

- Create an ACL to deny all users to alter the "cluster" resource from 2 hosts:

```bash
 rpk acl create \
  --deny-principal 'User:*' \
  --deny-host 192.168.98.74,10.235.78.12 \
  --operation alter \
  --resource cluster

Created ACL for principal 'User:*' with host '10.235.78.12', operation 'Alter' and permission 'Deny'
Created ACL for principal 'User:*' with host '192.168.98.74', operation 'Alter' and permission 'Deny'
```

As you can see, a single command may result in multiple ACLs being created. You an also create ACLs denying and allowing principals access:

```bash
 rpk acl create \
--resource cluster \
--allow-host 168.72.98.52 \
--deny-host 169.78.31.9 \
--deny-principal 'User:david' \
--allow-principal 'User:Alex' \
--allow-principal 'User:Ben' 

Created ACL for principal 'User:david' with host '169.78.31.9', operation 'All' and permission 'Deny'
Created ACL for principal 'User:Alex' with host '168.72.98.52', operation 'All' and permission 'Allow'
Created ACL for principal 'User:Ben' with host '168.72.98.52', operation 'All' and permission 'Allow'
```

**Listing ACLs**

`rpk acl list` allows you to list and filter existing ACLs.

- List all ACLs:

```bash
 rpk acl list

  PRINCIPAL     HOST           OPERATION  PERMISSION TYPE  RESOURCE TYPE  RESOURCE NAME  
                
  User:Charlie  *              All        Allow            Topic          pings          
  User:*        192.168.98.74  Alter      Deny             Cluster        kafka-cluster  
  User:david    169.78.31.9    All        Deny             Cluster        kafka-cluster  
  User:Ben      168.72.98.52   All        Allow            Cluster        kafka-cluster  
  User:*        10.235.78.12   Alter      Deny             Cluster        kafka-cluster  
  User:Alex     168.72.98.52   All        Allow            Cluster        kafka-cluster 
```

- List all ACLs for a specific principal:

```bash
 rpk acl list --principal 'User:Ben'

  PRINCIPAL  HOST          OPERATION  PERMISSION TYPE  RESOURCE TYPE  RESOURCE NAME  
             
  User:Ben   168.72.98.52  All        Allow            Cluster        kafka-cluster  
```

- List all ACLs that deny principals to alter a resource:

```bash
 rpk acl list \
  --permission deny \
  --operation alter

  PRINCIPAL  HOST           OPERATION  PERMISSION TYPE  RESOURCE TYPE  RESOURCE NAME  
             
  User:*     192.168.98.74  Alter      Deny             Cluster        kafka-cluster  
  User:*     10.235.78.12   Alter      Deny             Cluster        kafka-cluster
```

**Deleting ACLs**

`rpk acl delete` allows you to delete ACLs. It's important to note, however, that wildcard values such as `any` for operations and permissions, as well as `*` for hosts and resources may result in the deletion filters matching more than one ACL.

- Delete all ACLs for a specific user targeting a specific resource:

```bash
 rpk acl delete --deny-principal 'User:david' --resource cluster

  DELETED  PRINCIPAL   HOST         OPERATION  PERMISSION TYPE  RESOURCE TYPE  RESOURCE NAME  ERROR MESSAGE  
           
  yes      User:david  169.78.31.9  All        Deny             Cluster        kafka-cluster  None
```

- Delete all ACLs granting a principal permissions to all operations from a host:

```bash
 rpk acl delete \
  --allow-principal 'User:Ben' \
  --allow-host 168.72.98.52 \
  --operation all
```
