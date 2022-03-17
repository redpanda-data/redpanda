# Redpanda Operator

![Version: 0.1.0](https://img.shields.io/badge/Version-0.1.0-informational?style=flat-square) ![Type: application](https://img.shields.io/badge/Type-application-informational?style=flat-square) ![AppVersion: v21.3.4](https://img.shields.io/badge/AppVersion-v21.3.4-informational?style=flat-square)

## Installation

### Prerequisite

To deploy operator with webhooks (enabled by default) please install
cert manager. Please follow
[the installation guide](https://cert-manager.io/docs/installation/)

The cert manager needs around 1 minute to be ready. The helm chart
will create Issuer and Certificate custom resource. The
webhook of cert-manager will prevent from creating mentioned
resources. To verify that cert manager is ready please follow
[the verifying the installation](https://cert-manager.io/docs/installation/kubernetes/#verifying-the-installation)

The operator by default exposes metrics endpoint. By leveraging prometheus
operator ServiceMonitor custom resource metrics can be automatically
discovered.

1. Install Redpanda operator CRDs:

```sh
kubectl apply -k 'https://github.com/redpanda-data/redpanda/src/go/k8s/config/crd?ref=v21.3.4'
```

> The CRDs are decoupled from helm chart, so that helm release can be
> removed without cascading deletion of underling custom resources.
> Other argument for decoupling is that helm cli can incorrectly
> patch the Custom Resource Definition.

### Helm installation

1. Install the Redpanda operator:

> The example command should be invoked from `src/go/k8s/helm-chart/charts`

```sh
helm install --namespace redpanda-system --create-namespace redpanda-system ./redpanda-operator
```

Alternative installation with kube-prometheus-stack that includes prometheus operator CRD
```sh
helm install --dependency-update \
--namespace redpanda-system \
--set monitoring.enabled=true \
--create-namespace redpanda-operator ./redpanda-operator
```

Other instruction will be visible after installation.

## Values

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| affinity | object | `{}` | Allows to specify affinity for Redpanda Operator PODs |
| config.apiVersion | string | `"controller-runtime.sigs.k8s.io/v1alpha1"` |  |
| config.health.healthProbeBindAddress | string | `":8081"` |  |
| config.kind | string | `"ControllerManagerConfig"` |  |
| config.leaderElection.leaderElect | bool | `true` |  |
| config.leaderElection.resourceName | string | `"aa9fc693.vectorized.io"` |  |
| config.metrics.bindAddress | string | `"127.0.0.1:8080"` |  |
| config.webhook.port | int | `9443` |  |
| configurator.pullPolicy | string | `"IfNotPresent"` | Define the pullPolicy for Redpanda configurator image |
| configurator.repository | string | `"vectorized/configurator"` | Repository that Redpanda configurator image is available |
| configurator.tag | string | `"{{ .Chart.AppVersion }}"` | Define the Redpanda configurator container tag |
| clusterDomain | string | `cluster.local` | Defines Kubernetes Cluster Domain |
| fullnameOverride | string | `""` | Override the fully qualified app name |
| image.pullPolicy | string | `"IfNotPresent"` | Define the pullPolicy for Redpanda Operator image |
| image.repository | string | `"vectorized/redpanda-operator"` | Repository that Redpanda Operator image is available |
| image.tag | string | `"{{ .Chart.AppVersion }}"` | Define the Redpanda Operator container tag |
| imagePullSecrets | list | `[]` | Redpanda Operator container registry pullSecret (ex: specify docker registry credentials) |
| labels | string | `nil` | Allows to assign labels to the resources created by this helm chart |
| logLevel | string | `"info"` | Set Redpanda Operator log level (debug, info, error, panic, fatal) |
| monitoring | object | `{"enabled":false}` | Add service monitor to the deployment |
| nameOverride | string | `""` | Override name of app |
| nodeSelector | object | `{}` | Allows to schedule Redpanda Operator on specific nodes |
| podAnnotations | object | `{}` | Allows setting additional annotations for Redpanda Operator PODs |
| podLabels | object | `{}` | Allows setting additional labels for for Redpanda Operator PODs |
| rbac.create | bool | `true` | Specifies whether the RBAC resources should be created |
| replicaCount | int | `1` | Number of instances of Redpanda Operator |
| resources | object | `{}` | Set resources requests/limits for Redpanda Operator PODs |
| serviceAccount.create | bool | `true` | Specifies whether a service account should be created |
| serviceAccount.name | string | `nil` | The name of the service account to use. If not set name is generated using the fullname template |
| tolerations | list | `[]` | Allows to schedule Redpanda Operator on tainted nodes |
| webhook.enabled | bool | `true` |  |
