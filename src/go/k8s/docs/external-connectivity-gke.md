# Cluster with external connectivity on GKE

1. Create three node cluster with enough CPUs
```
gcloud container clusters create redpanda --machine-type e2-standard-4 --zone us-central1-c --project <yourproject>
```
Note: in time of writing this, 1.18 cluster on regular channel has a bug regarding nodeports. Consider using 1.19 cluster, e.g. via `--cluster-version 1.19.8-gke.1600` or latest 1.19
### - Make it possible to connect to cluster from kubectl
```
gcloud container clusters get-credentials redpanda --zone us-central1-c --project <yourproject>
```
### - Install cert-manager https://cert-manager.io/docs/installation/kubernetes/
### - Install latest redpanda operator
```
kubectl apply -k https://github.com/redpanda-data/redpanda/src/go/k8s/config/default
```
### - Install cluster with external connectivity
```
kubectl apply -f config/samples/external_connectivity.yaml
```
### - Get value of nodeport used for exposing redpanda nodes
```
kubectl get service external-connectivity-external -o=jsonpath='{.spec.ports[0].nodePort}'
30249
```
### - Use the nodeport to create firewall rule allowing traffic to it. In this example, the port is 30249, this will differ on your cluster
```
gcloud compute firewall-rules create redpanda-nodeport --allow tcp:30249 --project <yourproject>
```
### - Get broker addresses
```
kubectl get clusters external-connectivity -o=jsonpath='{.status.nodes.external}'
["34.121.167.159:30249","34.71.125.54:30249","35.184.221.5:30249"]
```
### - Query cluster info
```
rpk --brokers 34.121.167.159:30249,34.71.125.54:30249,35.184.221.5:30249 cluster info
  Redpanda Cluster Info

  0 (34.121.167.159:30249)  (No partitions)

  1 (34.71.125.54:30249)    (No partitions)

  2 (35.184.221.5:30249)    (No partitions)
```
### - Create topic
```
rpk --brokers 34.121.167.159:30249,34.71.125.54:30249,35.184.221.5:30249 topic create chat-rooms -p 5
Created topic 'chat-rooms'.
You may check its config with

rpk topic describe 'chat-rooms'
```