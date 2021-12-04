---
title: Using configuration parameters with a Custom Resource
order: 5
---
# Using configuration parameters with a Custom Resource

You can use the Redpanda operator to create clusters based on custom resource (CR) files.
After you install the Redpanda operator, you can apply the cluster CR that contains the basic specifications of the Redpanda cluster,
including the cluster name and namespace, and the configuration for the APIs like the Admin, Kafka, and Pandaproxy API.

The [Kubernetes Quick Start Guide](/docs/quick-start-kubernetes) walks you through the steps to set up the Redpanda operator and a Redpanda cluster.

In addition to the basic cluster specifications, you can add other parameters that are typically defined in the Redpanda [configuration file](/docs/configuration).
The custom parameters are added to the `additionalConfiguration` section of the CR.

For example, using the `single-node` sample from [our CR sample files](https://github.com/vectorizedio/redpanda/tree/dev/src/go/k8s/config/samples), here we add some custom parameters in the `additionalConfiguration` section:

> **_Notes_**
  - Versioning is not supported for the `additionalConfiguration` parameters
  - Unsupported key names cause Redpanda to fail on start
  - When you update the parameters you must manually restart the Redpanda pods for the changes to take effect.

## CR with additional parameters

```yaml
apiVersion: redpanda.vectorized.io/v1alpha1
kind: Cluster
metadata:
  name: one-node-cluster
spec:
  image: "docker.vectorized.io/vectorized/redpanda"
  version: "latest"
  replicas: 1
  resources:
    requests:
      cpu: 1
      memory: 1.2Gi
    limits:
      cpu: 1
      memory: 1.2Gi
  configuration:
    rpcServer:
      port: 33145
    kafkaApi:
    - port: 9092
    pandaproxyApi:
    - port: 8082
    adminApi:
    - port: 9644
    developerMode: true
  additionalConfiguration:
    redpanda.enable_idempotence: "true"
    redpanda.default_topic_partitions: "3"
    pandaproxy_client.retries: "10"
    schema_registry.schema_registry_api: "[{'name':'external','address':'0.0.0.0','port':8081}]"
```

 At the bottom we see a map, `additionalConfiguration`. These are parameters that are currently not part of the Custom Resource, but are passed to the Redpanda configuration (`redpanda.yaml`).

 The format of each entry is `<subsystem>.<property_name>: <value>`. Examples of subsystems, as seen above, are `redpanda`, `pandaproxy_client`, and `schema_registry`. The value can be a single value or a YAML/JSON-encoded string, as is the case with the `schema_registry` parameter in the example.

 When you apply this CR, the Redpanda logs show:

 ```yaml
 pandaproxy_client:
  ..
  retries: 10
redpanda:
  ..
  default_topic_partitions: 3
  enable_idempotence: true
  ..
...
schema_registry:
  schema_registry_api:
    - address: 0.0.0.0
      name: external
      port: 8081
```
