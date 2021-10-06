---
title: Vectorized Cloud Quick Start Guide - BYOC
order: 0
---

# Vectorized Cloud Quick Start Guide - BYOC

Want a managed-cloud solution for data streaming that you can host in your own cloud infrastructure?

Vectorized Cloud BYOC (Bring Your Own Cloud) gives you Redpanda services in your cloud that our dedicated team of engineers monitor and manage for you.
You get to handle the cloud contracts and data security, and we make sure you get the best data streaming solution available.

> **_Note_**: If you want Vectorized to manage the cloud infrastructure as well, [create a fully managed cluster](/docs/quick-start-cloud-managed).

After you set up the cluster, you'll get an endpoint and security credentials to allow your clients to access the cluster.
You can use PandaProxy HTTP requests to produce and consume on the cluster,
but we also give you a graphical dashboard with real-time metrics, basic topic management, and security controls for cluster and data access.

## Sign in to Vectorized Cloud

You can register for an account at [vectorized.cloud](vectorized.cloud) with an email address and password or with a Google account.

## Add a namespace

Vectorized Cloud clusters are grouped into namespaces to help you organize your clusters in groups for convenience,
but cluster names have to be unique across the whole organization.

Click **Add namespace** to create a namespace and give it a name.

## Add a cluster

In your namespace, click **Add cluster** and then choose which type of cluster to add:

First, you create the cluster in the Vectorized Cloud UI.

1. Enter a name for the cluster.
1. Select a cloud provider: **AWS** or **GCP**
1. Select a region in the provider.
1. Select the storage type for your cluster:
    - Local - 
    - Persistent standard - 
    - Persistent performance -
1. If you want the cluster to use one of your public certificates, select **Enable public certificates**.
1. If you want the Kafka API to use mTLS, select **Use mTLS for Kafka API**.

After the cluster is created in Vectorized Cloud, click **Setup cluster security** and follow the instructions to create the cluster in your cloud account.
The steps include:

1. Define the project ID, instance type, and number of nodes for your cluster.
1. Download and extract a zip archive of Terraform files.
1. In the extracted directory, run the provided terraform command, in the format of:
    - GCP - `terraform init && terraform apply -auto-approve && gcloud container clusters get-credentials --project $PROJECT_ID <cluster_name> --region <region> && kubectl apply -f ./cluster-manifest.yaml`
    - AWS - `terraform init && terraform apply -auto-approve && aws eks update-kubeconfig --region <region> --name <cluster_name> && kubectl apply -f ./cluster-manifest.yaml`

## Create a service account

Before you can connect to the cluster you have to create a service account for user authentication.

To create a service account with full permissions on the cluster:

1. Go to the **Security** section of the cluster.
2. In **Service accounts**, click **Add service account**.
3. Name the service account and click **Create**.
    The password for the account is automatically generated for the account.
    Make sure that you copy the password and keep it safe.
    You cannot show the password again or change it.
4. In the **ACL** section, click **Add ACL**.
5. Select the service account and permissions that you want it to have.
    To give the account permissions on all topics and groups, enter `*` for the topic and groups IDs.
6. Click **Create**.

Now you can use the service account in your cluster connections.

## Get the connection resources for the cluster

When the cluster is in **Running** state, click on the cluster to see the cluster management actions.

You can find the connection details for your cluster in the **Topics** section.

- Cluster host addresses - [Clients](/docs/faq/) use these addresses to connect to the Kafka API.
- Pandaproxy host - You can use [REST API calls](https://vectorized.io/blog/pandaproxy/) to produce and consume on the cluster.

Connections to the cluster require TLS authentication with the TLS certificates in the **Security** section.

- Kafka API TLS - Download these certificates for `rpk` cluster management and Kafka client connections.
- PandaProxy API TLS - Download these certificates for REST API connections.

## Test the connection

Now you can connect to the cluster and verify that it is ready for you to use.
When you connect to the cluster with [`rpk` commands](/docs/rpk-commands) you need to include:

- The cluster host addresses that you copied from the **Topics** section
- The path to the Kafka API TLS certificates that you downloaded from the **Security** section
- The username and password of the service account

Here's an example of an `rpk` command:

```
rpk cluster info /
--brokers <broker_address>,<broker_address>... /
--tls-key Downloads/cluster.key /
--tls-cert Downloads/cluster.crt /
--tls-truststore Downloads/ca.crt /
--sasl-mechanism "SCRAM-SHA-256" /
--user '<username>' --password '<password>'
```

## Manage the cluster

In the cluster, you can:

- View cluster metrics

    In the **Metrics** section you can see:
    
    - Latency
    - Kafka API Throughput
    - HTTP Errors
    - Intracluster Rpc Errors
    - Kafka API Errors

- Create and delete topics

    In the **Topics** section you can add topics, review their configuration details, and delete topics.

- Manage cluster security

    In the **Security** section you can add service accounts
    and define ACLs that control the access that those accounts have to individual topics and groups, and to the cluster itself.

- Get code snippets

    The **Code snippets** section shows samples of code to use in your clients to create topics, producers, and consumers.

For other management workflows, connect directly to the cluster.

## Manage Vectorized Cloud users

Manage your Vectorized Cloud users in the Settings menu (<icon type="settings"/>).
When you add a user, we'll send them an email so they can set up an account.

You can give each user access to:

- View the clusters in your account- **Read**
- View, add, and configure clusters - **Write**
- View, add and configure clusters, and manage account users - **Admin**

## What's next?

Congrats! You now have a Redpanda cluster that you can use to consume and produce data.

- Our [FAQ](/docs/faq) page shows all of the clients that you can use to do streaming with Redpanda.
     (Spoiler: Any Kafka-compatible client!)
- We have specific tips on [connecting to Redpanda with NodeJS](/docs/guide-nodejs/).