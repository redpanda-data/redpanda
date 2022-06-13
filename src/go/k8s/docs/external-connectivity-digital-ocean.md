# Cluster with external connectivity on Digital Ocean

1. Create three node cluster with enough CPUs

```
doctl kubernetes cluster create test-cluster --wait --size s-2vcpu-4gb
```

By default, kubeconfig will be updated.

### - Install cert-manager

You can follow the https://cert-manager.io/docs/installation/kubernetes/ or
run the following command:

```
helm repo add jetstack https://charts.jetstack.io && \
helm repo update && \
helm install \
  cert-manager jetstack/cert-manager \
  --namespace cert-manager \
  --create-namespace \
  --version v1.4.4 \
  --set installCRDs=true
```

### - Install latest redpanda operator

```
VERSION=v21.4.15
kubectl apply -k 'https://github.com/redpanda-data/redpanda/src/go/k8s/config/crd?ref=$VERSION'
helm repo add redpanda https://charts.vectorized.io/ && \
helm repo update \
helm install \
  --namespace redpanda-system \
  --create-namespace redpanda-operator \
  --version $VERSION \
  redpanda/redpanda-operator
```

### - Install cluster with external connectivity

```
VERSION=v21.4.15
kubectl apply -f https://raw.githubusercontent.com/redpanda-data/redpanda/$VERSION/src/go/k8s/config/samples/external_connectivity.yaml
```

### - Get broker addresses

```
kubectl get clusters external-connectivity -o=jsonpath='{.status.nodes.external}'
["167.172.103.87:31335","167.172.103.146:31335","167.172.103.227:31335"]
```

### - Query cluster info

```
rpk --brokers 167.172.103.87:31335,167.172.103.146:31335,167.172.103.227:31335 cluster info
  Redpanda Cluster Info

  0 (167.172.103.87:31335)   (No partitions)

  1 (167.172.103.146:31335)  (No partitions)

  2 (167.172.103.227:31335)  (No partitions)
```

### - Create topic

```
rpk --brokers 167.172.103.87:31335,167.172.103.146:31335,167.172.103.227:31335 topic create chat-rooms -p 5
Created topic 'chat-rooms'.
You may check its config with

rpk topic describe 'chat-rooms'
```
