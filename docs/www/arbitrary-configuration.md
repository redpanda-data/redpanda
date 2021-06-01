---
title: Arbtrary configuration parameters for the Redpanda Operator
order: 5
---
# Arbitrary configuration

Before continuing please go over the [Kubernetes Quick Start Guide](/docs/kubernetes-deployment) and follow the steps for setting up the Redpanda Operator and optionally a Redpanda cluster.

Once the Redpanda Operator is installed a Redpanda cluster can be created by *applying* a Cluster Custom Resource (CR) containing the specification of the desired Redpanda cluster. The CR includes the desired Redpanda Cluster name, namespace, and configuration for the Admin, Kafka, Pandaproxy API, and others.

The Cluster definition includes parameters that are commonly needed to configure a Redpanda
Cluster. However, Redpanda offers a broad range of parameters ([Custom configuration](/docs/custom-configuration)) not all of which are included in the Cluster definition.

For this reason, we introduced a parameter `additionalConfiguration` that allows you to include
arbitrary configuration parameters not included in the CR. For example, consider the `single-node` sample found in [our sample files](https://github.com/vectorizedio/redpanda/tree/dev/src/go/k8s/config/samples) with `additionalConfiguration` added: 

## Sample configuration

```yaml
apiVersion: redpanda.vectorized.io/v1alpha1
kind: Cluster
metadata:
  name: one-node-cluster
spec:
  image: "vectorized/redpanda"
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

 Once the above specification is applied, the Redpanda logs should include

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

## Limitations

 When using this capability keep in mind some limitations:
 - versioning is not supported for the `additionalConfiguration` parameters
 - key names not supported by Redpanda will lead to failure on start
 - updating this map requires a manual restart of the Redpanda pods 
