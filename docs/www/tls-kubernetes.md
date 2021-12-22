---
title: TLS for Kubernetes
order: 0
---

# TLS for Kubernetes

Talking over the internet with no encryption is something unthinkable these days. With Redpanda you can help secure your data by enabling [Transport Layer Security (TLS)](https://en.wikipedia.org/wiki/Transport_Layer_Security) across your clusters, with just a few configurations. 

If you haven’t, check our guide about [Encryption, authorization & authentication](https://vectorized.io/docs/acls/).

Here we will dive deeper into how we can configure TLS inside a Kubernetes cluster.

The Custom Resource Definition (CRD) of a Redpanda cluster includes three APIs: Kafka, Admin, and Pandaproxy. Each API can support TLS for server authentication and mutual TLS (mTLS) for client authentication.

TLS allows us to establish a secure connection between a client and a broker in which communication is encrypted. 

mTLS, or 2-way TLS, is an authentication method in which the client and the server must both present certificates. This way both can decide whether the other party can be trusted by ensuring they are who they claim to be.

## Getting started

To demonstrate how to get up and running with TLS in Kubernetes, we’ll create a sample configuration file for a one-node cluster. We'll call it `tls_example.yaml` but feel free to name it however you like. You can save it wherever `kubectl` can read it from.

To enable TLS, we need to add a listener entry under the API fields in our `yaml` configuration file, and then specify the subfield `tls` as `true` so we can enable it.

For example, check that `kafkaApi` now has a new flag that says `tls: enabled: true`.

Save this file as `tls_example.yaml`:

```yaml
apiVersion: redpanda.vectorized.io/v1alpha1
kind: Cluster
metadata:
 name: cluster-sample-tls
spec:
 image: "vectorized/redpanda"
 version: "latest"
 replicas: 1
 resources:
   requests:
     cpu: 1
     memory: 1.2G
   limits:
     cpu: 1
     memory: 1.2G
 configuration:
   rpcServer:
     port: 33145
   kafkaApi:
    - port: 9092
      tls:
        enabled: true
   pandaproxyApi:
    - port: 8082
      tls:
        enabled: true
   adminApi:
   - port: 9644
   developerMode: true
```

We’ll use the Kafka API to go through the TLS configuration, but you can use similar settings to set up TLS for the other APIs. For this guide, we assume external connectivity is disabled unless otherwise stated. If you want to enable external connectivity, check [this guide](https://vectorized.io/docs/kubernetes-external-connect/) that we wrote about it. 

The operator supports a single listener with TLS per API. Additionally, if you have two listeners for the same API and one of them has external connectivity enabled, the operator assumes that the other listener also has TLS enabled.

Providing an issuer or certificate to the operator is only available for the Kafka API. 

### Client Authorization (mTLS)

When you enable TLS, the client has the option of verifying the identity of the server, but to do this the client must have the CA’s certificate available in a trusted store.

We can then enable mTLS on the same listener by setting `requireClientAuth` to `true`.

So for example, we can edit our previous `yaml` file to add this new setting: 

```yaml
kafkaApi:
    - port: 9092
    tls:
        enabled: true
        requireClientAuth: true
```

When you enable mTLS, you must specify a key-pair (certificate and key). As we will later see, the operator can also generate the key-pair.

When you finish setting up your configuration, apply it by running:

```bash
kubectl apply \
-f <your-file-path/tls_example.yaml>
```

Remember to change to the actual file path that you used. 


## Managing Certificates

In this section, we discuss how the certificates are generated, what the renewal process looks like, and how you can view your certificates.

### Certificates Creation

The Redpanda operator uses [cert-manager](https://cert-manager.io) to generate certificates. 

By default, when you enable TLS, the Redpanda operator generates a root certificate for each API and uses that to generate leaf certificates for the nodes and client. The section below shows you how to check which certificates were generated. 

For the Kafka API, the operator also offers two options: 

- Provide an existing certificate issuer.
   
- Provide a node certificate.
   

This functionality is only available for the Kafka API.

To provide an existing certificate issuer, we need to add the `issuerRef` parameter to our configuration file:


```yaml
    kafkaApi:
    - port: 9092
      tls:
        enabled: true
        issuerRef:
          name: <issuer-name>
          kind: ClusterIssuer
```

Our other option is to provide a particular certificate (as a secret) as the node certificate. To do this, we need to add the `nodeSecretRef` field:

```yaml
    kafkaApi:
    - port: 9092
      tls:
        enabled: true
        nodeSecretRef:
          name: <cluster-tls-node-certificate>
          namespace: given-cert
```

If the secret is in a different namespace than the Redpanda cluster, the operator copies it over.

### Reading/Inspecting the certificates

When you enable TLS, the Redpanda operator generates a root certificate for the Kafka API that is consecutively used to generate a certificate for the Kafka API listener. 

If client authentication (mTLS) is enabled, an additional certificate is generated. Each certificate is stored in a Kubernetes secret resource under the same namespace as the Redpanda cluster. The certificate name is the same as that of the corresponding secret.

The Kafka API uses the following certificates:

Root certificate
* Used to generate the node and client certificates
* Secret: `|cluster|-kafka-root-certificate`

Node certificate
* The node certificate is automatically provided to Redpanda by the operator. The certificate secret is mounted as a volume that is consumed by Redpanda.
* Secret: `|cluster|-redpanda`

Client certificate (applicable only when mTLS is enabled)
* The client certificate is available for consumption. Depending on where the client is located, the client certificate secret can be mounted to local pods or be exported out of the Kubernetes cluster, as long as the Admin API is exposed externally.
* Secret: `|cluster|-user-client`
* Available for client use

The table below lists the certificates for each API. The client certificates are only generated and used for mTLS.


<table>
<tr>
<td><strong>Used by</strong>
</td>
<td><strong>Admin API</strong>
</td>
<td><strong>Kafka API</strong>
</td>
<td><strong>Pandaproxy API</strong>
</td>
</tr>
<tr>
<td>Node
</td>
<td>

|cluster|-admin-api-node

</td>
<td>

|cluster|-redpanda

</td>
<td>

|cluster|-proxy-api-node

</td>
</tr>
<tr>
<td>Client
</td>
<td>

|cluster|-admin-api-client

</td>
<td>

|cluster|-user-client

</td>
<td>

|cluster|-proxy-api-client

</td>
</tr>
</table>

Each certificate resource results in the creation of a secret resource of type `kubernetes.io/tls` that contains three components: `tls.crt`, `tls.key`, and `ca.crt`. 

You can check them by running:

```bash
kubectl describe secret |cluster|-admin-user-client
```

Which will return this: 

```
Name:  |cluster|-admin-user-client
..
Type:  kubernetes.io/tls
Data
====
tls.key:  .. bytes
tls.crt:  .. bytes
ca.crt:   .. bytes
```

If you want to check their content you can run:

```bash
kubectl get secret |cluster|-admin-user-client -o yaml
```

`ca.crt` is provided when using a self-signed Certificate Authority (CA), which is the default case but may not be provided as part of the above secret if using a well-known authority.


### Updating/ Renewing existing certificates

The renewing process is done automatically by cert-manager. You can check [how they do it on their website](https://cert-manager.io/docs/usage/certificate/#renewal).

The certificate duration is set by the operator to 5 years.

When this process happens, Redpanda reads the new certificates without needing to restart. 


## Using the certificates

Now that your server is secured, let's see how things work in from the perspective of the client. 

Each certificate has a Subject Alternative Name (SAN) that is set to the Fully Qualified Domain Name (FQDN) of the pods, with and without a wildcard prefix:

```
X509v3 Subject Alternative Name: 
    DNS:   |cluster|.default.svc.cluster.local,
    DNS: *.|cluster|.default.svc.cluster.local
```

The non-wildcard DNS is typically used for bootstrapping. The wildcarded name lets you access instances directly.

This assumes that the external connectivity is disabled. When external connectivity is enabled and a subdomain is provided, the certificates are produced for external use. For example, the SAN entries would become `*.<subdomain>` and `<subdomain>`. See the [Using names instead of external IPs](https://vectorized.io/docs/kubernetes-connectivity/#Using-names-instead-of-external-IPs) documentation for more information.

Depending on where the client is located in relation to the Kubernetes cluster running Redpanda, the client certificate secrets may be mounted to local pods or be exported and used directly (as files) inside or outside of the Kubernetes cluster, as long as the Kafka API is exposed to the external network.

There are multiple scenarios for consuming the certificates from a client application. For example, a client can be in the same Kubernetes cluster, can be in the same namespace, and can have a requirement for mTLS.

In this example, let's communicate with our cluster using `rpk`.

Depending on the the connectivity, first we need to get a list of brokers.

For convenience, let's save the result into a variable called `$BROKERS`:

For internal connectivity you can run:

```bash
export BROKERS=kubectl get clusters cluster -o=jsonpath='{.status.nodes.internal}' | jq -r 'join(",")'
```

For external connectivity you can run:

```bash
export BROKERS=kubectl get clusters cluster -o=jsonpath='{.status.nodes.external}' | jq -r 'join(",")'
```

After that we can run our `rpk` commands. 

If we only have TLS enabled, we just need to provide the CA: 

```
kubectl exec cluster-sample-tls-0 -- \
rpk cluster info  --brokers $BROKERS --tls-truststore=/etc/tls/certs/ca.crt 
```

For mTLS, we need to provide both the CA and the key-pair:

```
kubectl exec cluster-sample-tls-0 -- \
rpk cluster info  --brokers $BROKERS --tls-truststore=/etc/tls/certs/ca.crt --tls-key=/etc/tls/certs/tls.key --tls-cert=/etc/tls/certs/tls.crt
```

Typically, a client runs in a separate pod. If that pod is within the same Kubernetes cluster and namespace, the client pod can directly mount the certificate secrets:

```
spec:
  template:
    spec:
      volumes:
        - name: _tlscert_
          secret:
            secretName: |cluster|-redpanda
...
 containers:
        - name: client
          volumeMounts:
            - mountPath: /etc/tls/certs
              name: _tlscert_
```

You can find a working example with mTLS here: [https://github.com/vectorizedio/redpanda/tree/dev/src/go/k8s/tests/e2e/create-topic-with-client-auth](https://github.com/vectorizedio/redpanda/tree/dev/src/go/k8s/tests/e2e/create-topic-with-client-auth)

If the client pod runs in a separate namespace within the same Kubernetes cluster, the secret needs to be copied over.

Similarly, if the client is on a separate network, but accessible through an external IP address or name (see [https://vectorized.io/docs/kubernetes-connectivity](https://vectorized.io/docs/kubernetes-connectivity)) the secrets need to become available by copying over the resource or exporting the CA `ca.crt` and key-pair `tls.crt` and `tls.key` as files. For example, to retrieve tls.crt:

```
kubectl get secret |cluster|-user-client -o go-template='{{index .data "ca.crt"}}' | base64 -d - > tls.crt
```

Alternatively, you can retrieve the whole resource:

```
kubectl get secret |cluster|-_user-client_ --namespace=default -o yaml
```

Once the secrets are available as files, it is possible to use `rpk` to connect with the brokers. Here we consider external brokers, but they could be replaced with internal addresses if accessible.

For convenience, let's create a file and store our values there. We can pass this whole file as an argument for `rpk` instead of having to retype it all over again as flags. Let's name it `rpk-config.yaml`:


```
rpk:
  kafka_api:
    brokers:
      - 0.external-domain.:<nodeport>
      - 1.external-domain.:<nodeport>
      - 2.external-domain.:<nodeport>
    tls:
      key_file: tls.key
      cert_file: tls.crt
      truststore_file: ca.crt
```

Pass your saved `rpk-config.yaml` as a flag to `rpk` in order to talk to Redpanda:

```
rpk cluster info --config rpk-config.yaml
```

You can find more examples in our GitHub folder:
* [https://github.com/vectorizedio/redpanda/tree/dev/src/go/k8s/tests/e2e/create-topic-given-cert-secret](https://github.com/vectorizedio/redpanda/tree/dev/src/go/k8s/tests/e2e/create-topic-given-cert-secret)
* [https://github.com/vectorizedio/redpanda/tree/dev/src/go/k8s/tests/e2e/create-topic-given-issuer-with-client-auth](https://github.com/vectorizedio/redpanda/tree/dev/src/go/k8s/tests/e2e/create-topic-given-issuer-with-client-auth)