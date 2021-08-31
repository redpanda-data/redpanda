---
title: Vectorized Cloud Quick Start Guide
order: 0
---

Whether for small projects or large-scale deployments, why manage your Redpanda infrastructure yourself when you can leave the heavy-lifting to us?

Vectorized Cloud gives you cloud-based Redpanda services with security and stability without the need to manage your own clusters.
Either you can choose a _fully-managed_ cluster and have the cluster stay in our cloud infrastructure,
or you can use our _BYOC_ option to create clusters in your own cloud account with Terraform.
In both cases, our dedicated team of engineers monitor the cluster and take care of any maintenance issues that come up.

After you set up the cluster, you'll get an endpoint and security credentials to allow your clients to access the cluster.
You can use PandaProxy HTTP requests to manage your topics and partitions,
but we also give you a dashboard with real-time metrics, basic topic management, and security controls for cluster and data access.

## Sign in to Vectorized Cloud

You can register for an account at (cloud.vectorized.io) with an email address and password or with a Google account.

## Add a namespace

Vectorized Cloud clusters are grouped into namespaces to help you organize your clusters in groups for convenience.

Click **Add namespace** to create a namespace and give it a name.

## Add a cluster

In your namespace, click **Add cluster** and then choose which type of cluster to add:

<tabs>

  <tab id="Fully-managed">

    Fully-managed clusters are hosted in the Vectorized Cloud account.

    1. Enter a name for the cluster.
    2. Select a cloud provider: **AWS** or **GCP**
    3. Select a region in the provider.
    4. Select a size for the node instances.
    5. Enter the number of nodes for the cluster.
    6. Click **Create** to initiate cluster creation.

    After the cluster is created in Vectorized Cloud, you can click on the cluster in the namespace to see the cluster's connection details.

  </tab>

  <tab id="BYOC">

    After you create the BYOC cluster in Vectorized Cloud, you need to run some commands from a machine that has Terraform installed.

    1. Enter a name for the cluster.
    2. Select a cloud provider: **AWS** or **GCP**
    3. Select a region in the provider.

    After the cluster is created in Vectorized Cloud, you have to click **Setup cluster security** and follow the instructions to create the cluster in your cloud account.
    The steps include:

    1. Download and extract a zip archive of Terraform files.
    2. Define the variables for your cluster and download the Terraform variables file.
    3. Run `terraform init` and `terraform apply` to execute the Terraform actions.
    4. Connect to your cloud project.
    5. Download and apply the Kubernetes manifest to deploy the cluster.

  </tab>

</tabs>

## Connect to the cluster

When the cluster is in **Running** state, click on the cluster to see the cluster management actions.

You can find the connection details for your cluster in the **Topics** section.

- Cluster host addresses - Clients connect to these addresses for produce and consume actions.
- Pandaproxy host - You can use [REST API calls](https://vectorized.io/blog/pandaproxy/) to manage your cluster.

Connections to the cluster require TLS authentication with the TLS certificates in the **Security** section.

- Kafka API TLS - Use these certificates in your client data connections.
- Proxy API TLS - Use these certificates in REST API management connections.

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
        and define ACLs that control the access that those accounts have to individual topics, groups, and clusters.

    - Get code snippets

        The **Code snippets** section shows samples of code to use in your clients to create topics, producers, and consumers.

    For other management workflows, connect directly to the cluster.

## Manage Vectorized Cloud Users

Manage your Vectorized Cloud users in the Settings menu (add icon image).
When you add a user, we'll send them an email so they can set up an account.

You can give each user access to:

- View the clusters in your account- **Read**
- View, add, and configure clusters - **Write**
- View, add, and configure clusters, and manage account users - **Admin**
