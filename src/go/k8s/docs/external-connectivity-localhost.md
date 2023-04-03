1. Create kind cluster, be aware that port 30001 is expected to be free on your machine otherwise the creation will fail. Start new cluster with `kind create cluster --config docs/kind-external.yaml`
```
kind: Cluster
apiVersion: kind.x-k8s.io/v1alpha4
nodes:
- role: control-plane
  extraPortMappings:
  - containerPort: 30001
    hostPort: 30001
```
2. Install cert-manager
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
3. Get Latest version of the operator
```
export VERSION=$(curl -s https://api.github.com/repos/redpanda-data/redpanda/releases/latest | jq -r .tag_name)
```
4. Install CRDs
```
kubectl apply \
-k https://github.com/redpanda-data/redpanda/src/go/k8s/config/crd?ref=$VERSION
```
5. Install redpanda operator
```
helm repo add redpanda https://charts.vectorized.io/ && \
helm repo update && 
helm install \
redpanda-operator \
redpanda/redpanda-operator \
--namespace redpanda-system \
--create-namespace \
--version $VERSION
```
6. Create a namespace for your cluster
```
kubectl create ns chat-with-me
```
7. Install one node cluster
```
kubectl apply \
-n chat-with-me \
-f https://raw.githubusercontent.com/redpanda-data/redpanda/dev/src/go/k8s/config/samples/one_node_external.yaml
```
8. Etc/hosts
Make sure 0.local.rp is mapped to 127.0.0.1 on your system. It should contain line similar to this one:
```
127.0.0.1 0.local.rp
```
9. Create topic with 5 partitions
```
rpk --brokers localhost:30001 topic create chat-rooms -p 5
```
10. List topics
```
rpk --brokers localhost:30001 topic list
```
